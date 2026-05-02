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

# ====================== RESUMEN ANUAL (con FIFO) ======================
entradas = df[df['tipo'].isin(['compra', 'recompensa', 'minería']) & ~df['es_referral']].copy()
entradas['valor_unitario'] = entradas['total eur (tras pagar comisión)'].fillna(0) / entradas['cantidad'].replace(0, 1)

ventas_ganancia = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR']) & ~df['es_referral']].copy()

# FIFO para Ganancia Patrimonial
def calcular_fifo(ventas_df, entradas_df):
    inventario = []
    detalle = []

    for _, e in entradas_df.iterrows():
        inventario.append({
            'fecha': e['fecha'],
            'moneda': e['moneda'],
            'cantidad': float(e['cantidad']),
            'valor_unitario': float(e['valor_unitario'])
        })

    for _, venta in ventas_df.iterrows():
        cant_total = float(venta['cantidad'])
        total_ing = float(venta['total eur (tras pagar comisión)'])
        moneda = venta['moneda']
        fecha = venta['fecha']
        rest = cant_total

        while rest > 0 and inventario:
            posibles = [i for i in inventario if i['moneda'] == moneda and i['fecha'] <= fecha]
            if not posibles: break
            ent = sorted(posibles, key=lambda x: x['fecha'])[0]
            usado = min(rest, ent['cantidad'])

            coste = usado * ent['valor_unitario']
            ingreso = (usado / cant_total) * total_ing if cant_total > 0 else 0
            beneficio = ingreso - coste

            detalle.append({
                'Año': fecha.year,
                'Beneficio/Pérdida': round(beneficio, 2)
            })

            ent['cantidad'] -= usado
            if ent['cantidad'] <= 1e-8:
                inventario.remove(ent)
            rest -= usado
    return pd.DataFrame(detalle)

fifo_result = calcular_fifo(ventas_ganancia, entradas)

# Resumen Anual
resumen_anual = pd.DataFrame({'Año': range(2020, 2027)}).set_index('Año')
resumen_anual['Ganancia_Patrimonial'] = fifo_result.groupby('Año')['Beneficio/Pérdida'].sum()
resumen_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual = resumen_anual.fillna(0).round(2).reset_index()

# ====================== AGRUPADO POR AÑO (cantidad raw) ======================
ventas = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR']) & ~df['es_referral']]
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
    fifo_result.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)  # simplificado

    pd.DataFrame({
        'Concepto': ["Ganancia/Pérdida Patrimonial", "Rendimientos Recompensas", "Rendimientos Minería", "Referral Commission"],
        'Dónde declararlo': ["1800-1814 (F2)", "0033", "0033", "0304 (Base General)"],
        'Notas': ["Ver pestaña Agrupado por Año", "Valor de mercado", "Valor de mercado", "Base General"]
    }).to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print("✅ Archivo generado correctamente")
print(resumen_anual)
