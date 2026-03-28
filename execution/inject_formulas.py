import openpyxl

def inject():
    file_path = r'C:\Preencher planilha\FB - LOTTO V7.xlsx'
    wb = openpyxl.load_workbook(file_path)
    sheet = wb['032026']
    
    # We will inject the formulas from row 4 to row 100 for the creative tests table
    for row in range(4, 101):
        # Only inject if there constitutes a "CRIATIVO" name in column B
        # Actually it's better to just inject the formula anyway, but wrapped with IF(B{row}="","", ...)
        # so it stays clean, but the user requested formulas simply.
        # It's better to wrap in IFERROR or just inject it for all. Let's just put it simply.
        
        # HOOK VIEW 3'' (E)=VLOOKUP(B4, 'Dados Brutos'!A:L, 7, FALSE)
        sheet[f'E{row}'].value = f"=IFERROR(VLOOKUP(B{row}, 'Dados Brutos'!A:L, 7, FALSE), 0)"
        # Body View 75% (F)=VLOOKUP(B4, 'Dados Brutos'!A:L, 8, FALSE)
        sheet[f'F{row}'].value = f"=IFERROR(VLOOKUP(B{row}, 'Dados Brutos'!A:L, 8, FALSE), 0)"
        # CPM (G)=VLOOKUP(B4, 'Dados Brutos'!A:L, 10, FALSE)
        sheet[f'G{row}'].value = f"=IFERROR(VLOOKUP(B{row}, 'Dados Brutos'!A:L, 10, FALSE), 0)"
        # CTR (H)=VLOOKUP(B4, 'Dados Brutos'!A:L, 11, FALSE)
        sheet[f'H{row}'].value = f"=IFERROR(VLOOKUP(B{row}, 'Dados Brutos'!A:L, 11, FALSE), 0)"
        # CPC (I)=VLOOKUP(B4, 'Dados Brutos'!A:L, 12, FALSE)
        sheet[f'I{row}'].value = f"=IFERROR(VLOOKUP(B{row}, 'Dados Brutos'!A:L, 12, FALSE), 0)"
        # GASTO (J)=VLOOKUP(B4, 'Dados Brutos'!A:L, 9, FALSE)
        sheet[f'J{row}'].value = f"=IFERROR(VLOOKUP(B{row}, 'Dados Brutos'!A:L, 9, FALSE), 0)"
        # VENDAS (K)=SUMIF('Dados Brutos'!N:N, B4, 'Dados Brutos'!O:O)
        sheet[f'K{row}'].value = f"=SUMIF('Dados Brutos'!N:N, B{row}, 'Dados Brutos'!O:O)"
        # CPA (L)=IFERROR(VLOOKUP(B4, 'Dados Brutos'!M:P, 4, FALSE), 0)
        sheet[f'L{row}'].value = f"=IFERROR(VLOOKUP(B{row}, 'Dados Brutos'!M:P, 4, FALSE), 0)"
        
    wb.save(file_path)
    print("Formulas injected into 032026 sheet successfully.")

if __name__ == '__main__':
    inject()
