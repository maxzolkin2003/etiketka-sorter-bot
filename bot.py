import os
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
import pandas as pd
import fitz  # PyMuPDF
import re
import asyncio

TOKEN = os.getenv('TOKEN')
if not TOKEN:
    raise RuntimeError("TOKEN не установлен в переменных окружения")

def process_orders(excel_path, pdf_path):
    df = pd.read_excel(excel_path)

    df_cleaned = df[['Unnamed: 1', 'Unnamed: 3', 'Unnamed: 5', 'Unnamed: 7']].dropna().iloc[1:]
    df_cleaned.columns = ['Номер заказа', 'Артикул', 'Количество', 'Статус заказа']
    df_cleaned['Статус заказа'] = df_cleaned['Статус заказа'].str.strip()
    df_cleaned['Количество'] = pd.to_numeric(df_cleaned['Количество'], errors='coerce')

    canceled_orders = df_cleaned[df_cleaned['Статус заказа'] == 'Отменён в процессе обработки']
    canceled_order_numbers = canceled_orders['Номер заказа'].tolist()
    canceled_count = len(canceled_order_numbers)

    df_cleaned = df_cleaned[df_cleaned['Статус заказа'] != 'Отменён в процессе обработки']

    df_active = df_cleaned.copy()

    grouped = df_active.groupby('Номер заказа').apply(
        lambda x: pd.Series({
            'Артикулы с количеством': ', '.join(
                f"{row['Артикул']} — {int(row['Количество'])}" for _, row in x.iterrows()
            ),
            'Число артикулов': x['Артикул'].nunique(),
            'Общее количество': x['Количество'].sum(),
            'Основной артикул': x['Артикул'].iloc[0] if x['Артикул'].nunique() == 1 else ''
        })
    ).reset_index()

    grouped['Статус заказа'] = 'Готов к отправке'

    if canceled_count > 0:
        removed_info = pd.DataFrame({
            'Номер заказа': canceled_order_numbers,
            'Артикулы с количеством': [''] * canceled_count,
            'Статус заказа': [f'Удалено {canceled_count} строк со статусом "Отменён в процессе обработки"'] * canceled_count,
            'Число артикулов': [None] * canceled_count,
            'Общее количество': [None] * canceled_count,
            'Основной артикул': [''] * canceled_count
        })
    else:
        removed_info = pd.DataFrame(columns=grouped.columns)

    single_orders = grouped[grouped['Число артикулов'] == 1].copy()
    multi_orders = grouped[grouped['Число артикулов'] > 1].copy()

    single_orders = single_orders.sort_values(by=['Основной артикул', 'Общее количество'])

    final_result = pd.concat([single_orders, multi_orders, removed_info], ignore_index=True)

    final_result = final_result[['Номер заказа', 'Артикулы с количеством', 'Статус заказа']]

    excel_output_path = "sorted_shipment_orders_table.xlsx"
    final_result.to_excel(excel_output_path, index=False)

    pdf_document = fitz.open(pdf_path)
    pdf_text = ""
    for page_num in range(pdf_document.page_count):
        page = pdf_document.load_page(page_num)
        pdf_text += page.get_text("text")

    order_numbers_pdf_extracted = re.findall(r'\d{11}', pdf_text)
    order_numbers_sorted = final_result['Номер заказа'].dropna().astype(str).tolist()

    missing_from_pdf = set(order_numbers_sorted) - set(order_numbers_pdf_extracted)
    missing_from_table = set(order_numbers_pdf_extracted) - set(order_numbers_sorted)

    if missing_from_pdf:
        print("Номера заказов из таблицы, которые отсутствуют в PDF:")
        print(missing_from_pdf)
    if missing_from_table:
        print("Номера заказов из PDF, которые отсутствуют в таблице:")
        print(missing_from_table)

    pdf_writer = fitz.open()
    sorted_pages = []

    for order_num in order_numbers_sorted:
        page_added = False
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            text = page.get_text("text")
            if order_num in text and page_num not in sorted_pages:
                pdf_writer.insert_pdf(pdf_document, from_page=page_num, to_page=page_num)
                sorted_pages.append(page_num)
                page_added = True
                break
        if not page_added:
            print(f"Не удалось найти страницу для номера заказа {order_num}.")

    pdf_output_path = "sorted_shipment_orders_labels_final_corrected.pdf"
    pdf_writer.save(pdf_output_path)
    pdf_writer.close()

    return excel_output_path, pdf_output_path

async def handle_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message

    if 'excel_file' not in context.user_data:
        context.user_data['excel_file'] = None
    if 'pdf_file' not in context.user_data:
        context.user_data['pdf_file'] = None

    if message.document:
        file_name = message.document.file_name.lower()
        file = await message.document.get_file()

        if file_name.endswith('.xlsx'):
            path = f"./{file_name}"
            await file.download_to_drive(path)
            context.user_data['excel_file'] = path
            await message.reply_text("Excel файл получен.")
        elif file_name.endswith('.pdf'):
            path = f"./{file_name}"
            await file.download_to_drive(path)
            context.user_data['pdf_file'] = path
            await message.reply_text("PDF файл получен.")
        else:
            await message.reply_text("Пожалуйста, отправьте только Excel или PDF файл.")

    if context.user_data['excel_file'] and context.user_data['pdf_file']:
        await message.reply_text("Запускаю обработку файлов...")
        try:
            excel_out, pdf_out = process_orders(context.user_data['excel_file'], context.user_data['pdf_file'])
            await message.reply_document(open(excel_out, 'rb'), filename=excel_out)
            await message.reply_document(open(pdf_out, 'rb'), filename=pdf_out)
        except Exception as e:
            await message.reply_text(f"Ошибка при обработке: {e}")
        finally:
            try:
                os.remove(context.user_data['excel_file'])
                os.remove(context.user_data['pdf_file'])
                os.remove(excel_out)
                os.remove(pdf_out)
            except Exception:
                pass
            context.user_data['excel_file'] = None
            context.user_data['pdf_file'] = None

async def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.Document.ALL, handle_files))
    print("Бот запущен...")
    await app.run_polling()

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())

