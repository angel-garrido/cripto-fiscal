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
resumen_anual = pd.DataFrame({'Año': range(2020, 2027)}).set_index('Año')

# Ganancia Patrimonial (de ventas)
ventas = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR']) & ~df['es_referral']]
# Por ahora usamos total_eur como proxy (mejoraremos con FIFO después)
resumen_anual['Ganancia_Patrimonial'] = ventas.groupby('año')['total eur (tras pagar comisión)'].sum()

resumen_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()

resumen_anual = resumen_anual.fillna(0).round(2).reset_index()

# ====================== AGRUPADO POR AÑO Y CRIPTO ======================
ventas_agrup = ventas.groupby(['año', 'moneda']).agg({
    'cantidad': 'sum',
    'total eur (tras pagar comisión)': 'sum'
}).round(4).reset_index()

ventas_agrup.rename(columns={
    'año': 'Año',
    'moneda': 'Moneda',
    'cantidad': 'Cantidad Vendida',
    'total eur (tras pagar comisión)': 'Valor Transmisión'
}, inplace=True)

ventas_agrup['Tipo Contraprestación'] = 'N'
ventas_agrup['Valor Adquisición'] = 0.0
ventas_agrup['Beneficio/Pérdida'] = 0.0   # Pendiente de FIFO completo

# ====================== EXPORTAR ======================
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)
    ventas_agrup.to_excel(writer, sheet_name="Agrupado por Año", index=False)
    
    # FIFO Tracking (detalle)
    # Por ahora guardamos las ventas tal cual
    ventas[['fecha', 'moneda', 'cantidad', 'total eur (tras pagar comisión)']].to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)

    # Instrucciones
    pd.DataFrame({
        'Concepto': ["Ganancia/Pérdida Patrimonial", "Rendimientos Recompensas", "Rendimientos Minería", "Referral Commission"],
        'Dónde declararlo': ["Casillas 1800-1814 (F2)", "Casilla 0033", "Casilla 0033", "Casilla 0304 (Base General)"],
        'Notas': ["Ver pestaña Agrupado por Año", "Valor de mercado", "Valor de mercado", "Base General"]
    }).to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print("✅ Archivo generado")
print(resumen_anual)
