import pandas as pd

df = pd.read_excel("Cripto_Control_Fiscal.xlsx", sheet_name="Transacciones")

print("Columnas originales:", df.columns.tolist())

# Normalizar
df.columns = df.columns.str.strip().str.lower()
print("Columnas después de lower:", df.columns.tolist())

df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')

print("\nColumnas finales:", df.columns.tolist())

# Filtrar ventas
ventas = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR'])].copy()

print("\nNúmero de ventas:", len(ventas))
print("Columnas en ventas:", ventas.columns.tolist())

# Agrupado
agrupado = ventas.groupby([ventas['fecha'].dt.year, 'moneda']).agg({
    'cantidad': 'sum',
    'total_eur': 'sum'
}).round(4).reset_index()

agrupado.rename(columns={
    'fecha': 'Año',
    'moneda': 'Moneda',
    'cantidad': 'Cantidad Vendida',
    'total_eur': 'Valor Transmisión'
}, inplace=True)

agrupado['Tipo Contraprestación'] = 'N'

print("\n=== USDC 2025 ===")
print(agrupado[(agrupado['Año'] == 2025) & (agrupado['Moneda'] == 'USDC')])

# Guardar
with pd.ExcelWriter("resumen_fiscal_crypto_ESPANA.xlsx", engine='openpyxl') as writer:
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

print("\n✅ Archivo guardado")
