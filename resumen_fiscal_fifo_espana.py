import pandas as pd

archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA.xlsx"

df = pd.read_excel(archivo_entrada, sheet_name="Transacciones")

# Normalizar
df.columns = df.columns.str.strip().str.lower()
df.rename(columns={
    'precio de la cripto en eur': 'precio_unitario',
    'total eur (tras pagar comisión)': 'total_eur',
    'valoración fiscal (€)': 'valoracion_fiscal',
    'comentario': 'comentario'
}, inplace=True)

df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')

# Referral
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False) | \
                    df.get('comentario', '').astype(str).str.contains('referral', case=False, na=False)

# Solo ventas para agrupado
ventas = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR']) & ~df['es_referral']].copy()

# Agrupado simple y correcto por Año + Moneda
agrupado = ventas.groupby(['fecha', 'moneda']).agg({
    'cantidad': 'sum',
    'total_eur': 'sum'
}).reset_index()

agrupado['Año'] = agrupado['fecha'].dt.year
agrupado = agrupado.groupby(['Año', 'moneda']).agg({
    'cantidad': 'sum',
    'total_eur': 'sum'
}).round(4).reset_index()

agrupado.rename(columns={
    'moneda': 'Moneda',
    'cantidad': 'Cantidad Vendida',
    'total_eur': 'Valor Transmisión'
}, inplace=True)

agrupado['Tipo Contraprestación'] = 'N'
agrupado['Valor Adquisición'] = 0.0   # Se puede mejorar después con FIFO
agrupado['Beneficio/Pérdida'] = agrupado['Valor Transmisión']   # Temporal

print("Agrupado por Año - USDC 2025:")
print(agrupado[(agrupado['Año'] == 2025) & (agrupado['Moneda'] == 'USDC')])

# Guardar solo la pestaña que queremos revisar
with pd.ExcelWriter(archivo_salida, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

print("\n✅ Pestaña 'Agrupado por Año' actualizada")
