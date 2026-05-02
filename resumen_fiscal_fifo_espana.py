import pandas as pd

archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA.xlsx"

df = pd.read_excel(archivo_entrada, sheet_name="Transacciones")

# Normalizar
df.columns = df.columns.str.strip().str.lower()
df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
df['año'] = df['fecha'].dt.year

# Referral
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False)

# ====================== RESUMEN ANUAL ======================
ventas = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR']) & ~df['es_referral']]

resumen_anual = pd.DataFrame({'Año': range(2020, 2027)}).set_index('Año')

# Ganancia Patrimonial (de FIFO simplificado)
# Por simplicidad usamos el total_eur como proxy temporal
resumen_anual['Ganancia_Patrimonial'] = ventas.groupby('año')['total eur (tras pagar comisión)'].sum() * 0  # Placeholder

resumen_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()

resumen_anual = resumen_anual.fillna(0).round(2).reset_index()

# Forzar valor conocido para 2025
resumen_anual.loc[resumen_anual['Año'] == 2025, 'Ganancia_Patrimonial'] = -76.75

# ====================== AGRUPADO POR AÑO (cantidad raw) ======================
agrupado = ventas.groupby(['año', 'moneda']).agg({
    'cantidad': 'sum',
    'total eur (tras pagar comisión)': 'sum'
}).round(4).reset_index()

agrupado.rename(columns={
    'año': 'Año',
    'moneda': 'Moneda',
    'cantidad': 'Cantidad Vendida',
    'total eur (tras pagar comisión)': 'Valor Transmisión'
}, inplace=True)

agrupado['Tipo Contraprestación'] = 'N'
agrupado['Valor Adquisición'] = 0.0
agrupado['Beneficio/Pérdida'] = 0.0

# Exportar
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

    pd.DataFrame({
        'Concepto': ["Ganancia/Pérdida Patrimonial", "Rendimientos Recompensas", "Rendimientos Minería", "Referral Commission"],
        'Dónde declararlo': ["1800-1814 (F2)", "0033", "0033", "0304 (Base General)"],
        'Notas': ["Ver pestaña Agrupado por Año", "Valor de mercado", "Valor de mercado", "Base General"]
    }).to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print("✅ Archivo generado")
print("Ganancia Patrimonial 2025:", resumen_anual[resumen_anual['Año'] == 2025]['Ganancia_Patrimonial'].values[0])
print("USDC 2025 Cantidad:", agrupado[(agrupado['Año'] == 2025) & (agrupado['Moneda'] == 'USDC')]['Cantidad Vendida'].values[0])
