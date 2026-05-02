import pandas as pd

df = pd.read_excel("Cripto_Control_Fiscal.xlsx", sheet_name="Transacciones")

# Normalizar
df.columns = df.columns.str.strip().str.lower()
df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
df['año'] = df['fecha'].dt.year

# Referral
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False)

# ====================== RESUMEN ANUAL ======================
resumen_anual = pd.DataFrame({'Año': range(2020, 2027)}).set_index('Año')

# Ganancia Patrimonial (solo ventas de cripto)
ventas = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR']) & ~df['es_referral']]
resumen_anual['Ganancia_Patrimonial'] = ventas.groupby('año')['total eur (tras pagar comisión)'].sum()  # Temporal

resumen_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()

resumen_anual = resumen_anual.fillna(0).round(2).reset_index()

# Corregir Ganancia Patrimonial con FIFO básico (aprox)
# Por ahora dejamos el valor que tenías antes (-76.75) hasta tener FIFO completo
print("Resumen Anual (provisional):")
print(resumen_anual)

# ====================== AGRUPADO POR AÑO ======================
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

# Guardar
with pd.ExcelWriter("resumen_fiscal_crypto_ESPANA.xlsx", engine='openpyxl') as writer:
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

    pd.DataFrame({
        'Concepto': ["Ganancia/Pérdida Patrimonial", "Rendimientos Recompensas", "Rendimientos Minería", "Referral Commission"],
        'Dónde declararlo': ["1800-1814 (F2)", "0033", "0033", "0304 (Base General)"],
        'Notas': ["Ver pestaña Agrupado por Año", "Valor mercado", "Valor mercado", "Base General"]
    }).to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print("\n✅ Archivo generado")
print("Ganancia Patrimonial 2025:", resumen_anual[resumen_anual['Año'] == 2025]['Ganancia_Patrimonial'].values[0])
