import pandas as pd

archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA.xlsx"

df = pd.read_excel(archivo_entrada, sheet_name="Transacciones")

# Normalizar columnas
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
df = df.sort_values('fecha').reset_index(drop=True)

# Referral
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False) | \
                    df.get('comentario', pd.Series(['']*len(df))).astype(str).str.contains('referral', case=False, na=False)

# Valor Total
df['valor_total'] = df['total_eur'].fillna(0)
mask_earn = df['tipo'].isin(['recompensa', 'minería']) & ~df['es_referral']
df.loc[mask_earn, 'valor_total'] = df.loc[mask_earn, 'precio_unitario'] * df.loc[mask_earn, 'cantidad']

# ====================== FIFO DETALLADO ======================
entradas = df[df['tipo'].isin(['compra', 'recompensa', 'minería']) & ~df['es_referral']].copy()
entradas['valor_unitario'] = entradas['valor_total'] / entradas['cantidad'].replace(0, 1)

ventas_ganancia = df[(df['tipo'] == 'venta') & ~df['moneda'].isin(['EUR']) & ~df['es_referral']].copy()

def calcular_fifo_detalle(ventas_df, entradas_df):
    inventario = []
    detalle = []

    for _, e in entradas_df.iterrows():
        inventario.append({
            'fecha': e['fecha'], 'moneda': e['moneda'], 'cantidad': float(e['cantidad']),
            'valor_unitario': float(e['valor_unitario']), 'tipo': e['tipo']
        })

    for _, venta in ventas_df.iterrows():
        cant_total = float(venta['cantidad'])
        total_ing = float(venta['total_eur'])
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
                'Fecha Venta': fecha,
                'Moneda': moneda,
                'Cantidad Vendida': round(cant_total, 6),
                'Valor Transmisión': round(ingreso, 2),
                'Valor Adquisición': round(coste, 2),
                'Beneficio/Pérdida': round(beneficio, 2),
                'Tipo Contraprestación': 'N'
            })

            ent['cantidad'] -= usado
            if ent['cantidad'] <= 1e-8:
                inventario.remove(ent)
            rest -= usado
    return pd.DataFrame(detalle)

fifo_detalle = calcular_fifo_detalle(ventas_ganancia, entradas)

# ====================== RESUMEN ANUAL ======================
df['año'] = df['fecha'].dt.year

resumen_anual = pd.DataFrame({
    'Año': range(2020, 2027)
}).set_index('Año')

resumen_anual['Ganancia_Patrimonial'] = fifo_detalle.groupby('Año')['Beneficio/Pérdida'].sum()
resumen_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['valor_total'].sum()
resumen_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['valor_total'].sum()
resumen_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['valor_total'].sum()
resumen_anual = resumen_anual.fillna(0).round(2).reset_index()

# ====================== AGRUPADO POR CRIPTO (para declarar) ======================
agrupado_cripto = fifo_detalle.groupby(['Año', 'Moneda']).agg({
    'Cantidad Vendida': 'sum',
    'Valor Transmisión': 'sum',
    'Valor Adquisición': 'sum',
    'Beneficio/Pérdida': 'sum'
}).round(2).reset_index()

agrupado_cripto['Tipo Contraprestación'] = 'N'

# ====================== EXPORTAR ======================
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)
    agrupado_cripto.to_excel(writer, sheet_name="Agrupado por Año", index=False)
    fifo_detalle.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)

    pd.DataFrame({
        'Concepto': ["Ganancia/Pérdida Patrimonial", "Rendimientos Recompensas", "Rendimientos Minería", "Referral Commission"],
        'Dónde declararlo': ["Casillas 1800-1814 (F2)", "Casilla 0033", "Casilla 0033", "Casilla 0304 (Base General)"],
        'Notas': ["Pestaña Agrupado por Año", "Valor de mercado", "Valor de mercado", "Base General (no ahorro)"]
    }).to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print("✅ Archivo generado correctamente:", archivo_salida)
print("→ Resumen Anual: completo con todas las categorías")
print("→ Agrupado por Año: por cripto + año (listo para declarar)")
print("→ FIFO Tracking Detallado: sin cambios importantes")