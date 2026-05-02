import pandas as pd

archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA.xlsx"

df = pd.read_excel(archivo_entrada, sheet_name="Transacciones")

# Normalizar
df.columns = df.columns.str.strip().str.lower()
df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')

# Referral
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False)

# Resumen Anual
df['año'] = df['fecha'].dt.year
resumen_anual = pd.DataFrame({'Año': range(2020, 2027)}).set_index('Año')

# Ganancia Patrimonial (aprox por ahora)
ventas = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR']) & ~df['es_referral']]
resumen_anual['Ganancia_Patrimonial'] = 0.0  # Se puede mejorar con FIFO completo

resumen_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual = resumen_anual.fillna(0).round(2).reset_index()

# Agrupado por Cripto (correcto)
agrupado = ventas.groupby([ventas['fecha'].dt.year, 'moneda']).agg({
    'cantidad': 'sum',
    'total eur (tras pagar comisión)': 'sum'
}).round(4).reset_index()

agrupado.rename(columns={
    'fecha': 'Año',
    'moneda': 'Moneda',
    'cantidad': 'Cantidad Vendida',
    'total eur (tras pagar comisión)': 'Valor Transmisión'
}, inplace=True)

agrupado['Tipo Contraprestación'] = 'N'
agrupado['Valor Adquisición'] = 0.0   # Pendiente FIFO completo
agrupado['Beneficio/Pérdida'] = 0.0

# Guardar
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)
    
    pd.DataFrame({
        'Concepto': ["Ganancia/Pérdida Patrimonial", "Rendimientos Recompensas", "Rendimientos Minería", "Referral Commission"],
        'Dónde declararlo': ["1800-1814 (F2)", "0033", "0033", "0304 (Base General)"],
        'Notas': ["Usa pestaña Agrupado por Año", "", "", "Base General"]
    }).to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print("✅ Archivo generado correctamente")
print("USDC 2025 en 'Agrupado por Año':", agrupado[(agrupado['Año'] == 2025) & (agrupado['Moneda'] == 'USDC')]['Cantidad Vendida'].values[0])
