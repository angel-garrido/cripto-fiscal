import pandas as pd

# ====================== LECTURA ======================
df = pd.read_excel("Cripto_Control_Fiscal.xlsx", sheet_name="Transacciones")

# Normalizar
df.columns = df.columns.str.strip().str.lower()
df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')

# Filtrar solo ventas relevantes
ventas = df[
    (df['tipo'] == 'venta') & 
    ~df['moneda'].isin(['EUR']) 
].copy()

# Agrupado correcto por Año y Moneda
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
agrupado['Valor Adquisición'] = 0.0   # Se puede mejorar después
agrupado['Beneficio/Pérdida'] = 0.0

print("=== USDC 2025 en Agrupado por Año ===")
print(agrupado[(agrupado['Año'] == 2025) & (agrupado['Moneda'] == 'USDC')])

# Guardar
with pd.ExcelWriter("resumen_fiscal_crypto_ESPANA.xlsx", engine='openpyxl') as writer:
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)
    print("\n✅ Pestaña 'Agrupado por Año' actualizada correctamente")
