import asyncio
import os
import shutil
import datetime
import gc
import traceback
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import zipfile
from gspread_dataframe import set_with_dataframe
from playwright.async_api import async_playwright

# =================== CONFIGURAÇÕES ===================
DOWNLOAD_DIR = "/tmp/shopee_automation"
SPREADSHEET_ID = "1Ie3u58e-PT1ZEQJE20a6GJB-icJEXBRVDVxTzxCqq4c"
ABA_NOME = "Base"
OPS_ID = os.environ.get('OPS_ID')
OPS_SENHA = os.environ.get('OPS_SENHA')
# =====================================================

def rename_downloaded_file(DOWNLOAD_DIR, download_path):
    try:
        current_hour = datetime.datetime.now().strftime("%H")
        new_file_name = f"TO-Packed{current_hour}.zip"
        new_file_path = os.path.join(DOWNLOAD_DIR, new_file_name)
        
        if os.path.exists(new_file_path):
            os.remove(new_file_path)
            
        shutil.move(download_path, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
        return new_file_path
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        return None

def unzip_and_process_data(zip_path, extract_to_dir):
    try:
        unzip_folder = os.path.join(extract_to_dir, "extracted_files")
        os.makedirs(unzip_folder, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_folder)
        print(f"📂 Arquivo '{os.path.basename(zip_path)}' descompactado.")

        csv_files = [os.path.join(unzip_folder, f) for f in os.listdir(unzip_folder) if f.lower().endswith('.csv')]
        
        if not csv_files:
            print(f"⚠ Nenhum CSV encontrado no {zip_path}")
            shutil.rmtree(unzip_folder)
            return None

        print(f"📑 Lendo e unificando {len(csv_files)} arquivos CSV...")
        all_dfs = [pd.read_csv(file, encoding='utf-8') for file in csv_files]
        df_final = pd.concat(all_dfs, ignore_index=True)

        print("🔎 Aplicando filtros e ordem das colunas...")
        
        # LINHA SALVA-VIDAS: Imprime no terminal o nome exato de todas as colunas que vieram no CSV
        print(f"Colunas reais encontradas no CSV: {df_final.columns.tolist()}")
        
        colunas_para_manter = [
            "Order ID",          # Vai para Coluna A
            "SOC Received time", # Vai para Coluna B
            "Next Station",      # Vai para Coluna C
            "Current Station",   # Vai para Coluna D (Cópia exata garantida)
            "Outbound 3PL"       # Vai para Coluna E (Ajustado com maiúsculas corretas)
        ]
        
        df_final = df_final[colunas_para_manter]

        # Tratando a data recebida no SOC (Coluna B)
        df_final['SOC Received time'] = pd.to_datetime(df_final['SOC Received time'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y %H:%M:%S')

        shutil.rmtree(unzip_folder)
        return df_final
    except Exception as e:
        print(f"❌ Erro processando {zip_path}: {e}")
        return None

def update_google_sheet_with_dataframe(df_to_upload):
    if df_to_upload is None or df_to_upload.empty:
        print(f"⚠ Nenhum dado para enviar para a aba '{ABA_NOME}'.")
        return
        
    try:
        print(f"⬆ Enviando dados para a aba '{ABA_NOME}'...")

        df_to_upload = df_to_upload.fillna("").astype(str)

        scope = [
            "https://spreadsheets.google.com/feeds",
            'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive"
        ]
        if not os.path.exists("hxh.json"):
            raise FileNotFoundError("O arquivo 'hxh.json' não foi encontrado.")

        creds = Credentials.from_service_account_file("hxh.json", scopes=scope)
        client = gspread.authorize(creds)
        
        planilha = client.open_by_key(SPREADSHEET_ID)

        try:
            aba = planilha.worksheet(ABA_NOME)
        except gspread.exceptions.WorksheetNotFound:
            aba = planilha.add_worksheet(title=ABA_NOME, rows="1000", cols="20")
        
        aba.clear()
        set_with_dataframe(aba, df_to_upload)
        
        print(f"✅ Dados enviados com sucesso para '{ABA_NOME}'!")
    except Exception as e:
        import traceback
        print(f"❌ Erro ao enviar para Google Sheets na aba '{ABA_NOME}':\n{traceback.format_exc()}")

async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, 
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080"]
        )
        context = await browser.new_context(accept_downloads=True, viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        try:
            d1 = 'SoC_SP_Cravinhos'
            print("Realizando login...")
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=15000)
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill(OPS_ID)
            await page.locator('xpath=//*[@placeholder="Senha"]').fill(OPS_SENHA)
            await page.wait_for_timeout(5000)
            await page.get_by_role('button', name='Entrar').click(force=True)
            await page.wait_for_timeout(10000)
            
            try:
                if await page.locator('.ssc-dialog-close').is_visible():
                    await page.locator('.ssc-dialog-close').click()
            except:
                pass
            
            print("Navegando...")
            await page.goto("https://spx.shopee.com.br/#/orderTracking")
            await page.wait_for_timeout(8000)
            
            try:
                if await page.locator('.ssc-dialog-wrapper').is_visible():
                     await page.keyboard.press("Escape")
                     await page.wait_for_timeout(1000)
            except:
                pass

            print("Exportando... (TESTE)")

            await page.get_by_role('button', name='Exportar').click(force=True)
            await page.wait_for_timeout(5000)
            
            await page.get_by_text("Exportar Pedido Avançado").click()
            await page.wait_for_timeout(5000)
            await page.get_by_role("treeitem", name="SOC_Received", exact=True).click(force=True)
            await page.wait_for_timeout(5000)
            
            await page.get_by_text("+ adicionar à").nth(2).click()
            await page.wait_for_timeout(5000)

            await page.locator('xpath=/html/body/span[6]/div/div[1]/div/input').fill('SoC_SP_Cravinhos')
            await page.wait_for_timeout(5000)

            await page.locator('xpath=/html[1]/body[1]/span[6]/div[1]/div[2]/div[1]/ul[1]/div[1]/div[1]/li[1]').click()
            await page.get_by_role("button", name="Confirmar").click(force=True)
            
            print("Aguardando geração do relatório...")
            await page.wait_for_timeout(900000) 

            print("Baixando...")
            async with page.expect_download(timeout=150000) as download_info:
                await page.get_by_role("button", name="Baixar").first.click(force=True)
            
            download = await download_info.value
            download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path)
            print(f"Download concluído: {download_path}")

            renamed_zip_path = rename_downloaded_file(DOWNLOAD_DIR, download_path)
            
            if renamed_zip_path:
                final_dataframe = unzip_and_process_data(renamed_zip_path, DOWNLOAD_DIR)
                update_google_sheet_with_dataframe(final_dataframe)
                
                if final_dataframe is not None:
                    del final_dataframe
                    gc.collect()

        except asyncio.CancelledError:
            print("❌ ERRO FATAL: A execução foi cancelada externamente (Timeout do GitHub Actions ou cancelamento manual). O tempo de espera pode ter sido muito longo.")
        except KeyboardInterrupt:
            print("❌ ERRO FATAL: O script foi interrompido à força (Sinal do sistema).")
        except Exception as e:
            print(f"❌ ERRO FATAL durante a execução do Playwright: {e}")
            print("Detalhes técnicos do erro:")
            traceback.print_exc()
        finally:
            print("Iniciando encerramento seguro do navegador...")
            await browser.close()
            if os.path.exists(DOWNLOAD_DIR):
                shutil.rmtree(DOWNLOAD_DIR)
                print("Limpeza dos arquivos temporários concluída.")

if __name__ == "__main__":
    asyncio.run(main())
