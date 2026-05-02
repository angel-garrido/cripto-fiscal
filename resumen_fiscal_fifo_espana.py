import pandas as pd

df = pd.read_excel("Cripto_Control_Fiscal.xlsx", sheet_name="Transacciones")

# Normalizar
df.columns = df.columns.str.strip().str.lower()
df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')

# Referral
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False)

# Solo ventas para agrupado
ventas = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR']) & ~df['es_referral']].copy()

# Agrupado simple por cripto y año (Cantidad y Valor Transmisión correctos)
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
agrupado['Valor Adquisición'] = 0.0   # Pendiente de FIFO completo
agrupado['Beneficio/Pérdida'] = 0.0

print("=== AGRUPADO POR AÑO (USDC 2025) ===")
print(agrupado[(agrupado['Año'] == 2025) & (agrupado['Moneda'] == 'USDC')])

# Guardar
with pd.ExcelWriter("resumen_fiscal_crypto_ESPANA.xlsx", engine='openpyxl') as writer:
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)
    print("\n✅ Guardado en pestaña 'Agrupado por Año'")
