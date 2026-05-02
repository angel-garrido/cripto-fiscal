import pandas as pd

df = pd.read_excel("Cripto_Control_Fiscal.xlsx", sheet_name="Transacciones")

# Normalizar columnas
df.columns = df.columns.str.strip().str.lower()

df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')

print("Columnas disponibles:", df.columns.tolist())

# Filtrar ventas
ventas = df[
    (df['tipo'] == 'venta') & 
    ~df['moneda'].isin(['EUR'])
].copy()

# Agrupado usando el nombre real de la columna
agrupado = ventas.groupby([ventas['fecha'].dt.year, 'moneda']).agg({
    'cantidad': 'sum',
    'total eur (tras pagar comisión)': 'sum'   # ← Nombre correcto
}).round(4).reset_index()

agrupado.rename(columns={
    'fecha': 'Año',
    'moneda': 'Moneda',
    'cantidad': 'Cantidad Vendida',
    'total eur (tras pagar comisión)': 'Valor Transmisión'
}, inplace=True)

agrupado['Tipo Contraprestación'] = 'N'
agrupado['Valor Adquisición'] = 0.0
agrupado['Beneficio/Pérdida'] = 0.0

print("\n=== USDC 2025 ===")
usdc_2025 = agrupado[(agrupado['Año'] == 2025) & (agrupado['Moneda'] == 'USDC')]
print(usdc_2025)

# Guardar
with pd.ExcelWriter("resumen_fiscal_crypto_ESPANA.xlsx", engine='openpyxl') as writer:
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

print("\n✅ Pestaña actualizada")
