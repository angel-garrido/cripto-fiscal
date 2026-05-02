import pandas as pd

# ====================== CONFIGURACIÓN ======================
archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA.xlsx"

# Leer datos
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
df = df.sort_values('fecha').reset_index(drop=True)

# Referral
df['es_referral'] = (
    df['tipo'].str.contains('referral', case=False, na=False) |
    df['comentario'].astype(str).str.contains('referral', case=False, na=False)
)

# Valor Total
df['valor_total'] = df['total_eur'].fillna(0)
mask_earn = df['tipo'].isin(['recompensa', 'minería']) & ~df['es_referral']
df.loc[mask_earn, 'valor_total'] = df.loc[mask_earn, 'precio_unitario'] * df.loc[mask_earn, 'cantidad']

# Entradas FIFO
entradas = df[df['tipo'].isin(['compra', 'recompensa', 'minería']) & ~df['es_referral']].copy()
entradas['valor_unitario'] = entradas['valor_total'] / entradas['cantidad'].replace(0, 1)

# Ventas para Ganancia Patrimonial
ventas = df[df['tipo'] == 'venta'].copy()
ventas_ganancia = ventas[~ventas['moneda'].isin(['EUR']) & ~ventas['es_referral']].copy()

# ====================== FUNCIÓN FIFO ======================
def calcular_fifo_espana(ventas_df, entradas_df):
    inventario = []
    detalle = []

    for _, e in entradas_df.iterrows():
        inventario.append({
            'fecha': e['fecha'],
            'moneda': e['moneda'],
            'cantidad': float(e['cantidad']),
            'valor_unitario': float(e['valor_unitario']),
            'precio_unitario': float(e.get('precio_unitario', 0)),
            'tipo': e['tipo']
        })

    for _, venta in ventas_df.iterrows():
        cantidad_total = float(venta['cantidad'])
        total_transmision = float(venta['total_eur'])
        moneda = venta['moneda']
        fecha_venta = venta['fecha']
        restante = cantidad_total

        while restante > 0 and inventario:
            posibles = [item for item in inventario if item['moneda'] == moneda and item['fecha'] <= fecha_venta]
            if not posibles:
                break
            entrada = sorted(posibles, key=lambda x: x['fecha'])[0]
            usado = min(restante, entrada['cantidad'])

            coste = usado * entrada['valor_unitario']
            ingreso = (usado / cantidad_total) * total_transmision if cantidad_total > 0 else 0
            beneficio = ingreso - coste

            detalle.append({
                'Año': fecha_venta.year,
                'Fecha Venta': fecha_venta,
                'Moneda': moneda,
                'Cantidad Vendida': round(cantidad_total, 8),
                'Valor Transmisión': round(ingreso, 2),
                'Valor Adquisición': round(coste, 2),
                'Beneficio/Pérdida': round(beneficio, 2),
                'Tipo Contraprestación': 'N' if moneda != 'EUR' else 'F'
            })

            entrada['cantidad'] -= usado
            if entrada['cantidad'] <= 1e-8:
                inventario.remove(entrada)
            restante -= usado

    return pd.DataFrame(detalle)


# Ejecutar
fifo_detallado = calcular_fifo_espana(ventas_ganancia, entradas)

# ====================== AGRUPADO POR CRIPTO Y AÑO ======================
agrupado = fifo_detallado.groupby(['Año', 'Moneda']).agg({
    'Cantidad Vendida': 'sum',
    'Valor Transmisión': 'sum',
    'Valor Adquisición': 'sum',
    'Beneficio/Pérdida': 'sum'
}).reset_index()

agrupado = agrupado.round(2)

# ====================== EXPORTAR ======================
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    # Resumen general por año
    resumen_anual = agrupado.groupby('Año').agg({
        'Valor Transmisión': 'sum',
        'Valor Adquisición': 'sum',
        'Beneficio/Pérdida': 'sum'
    }).reset_index()
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)

    # Tabla principal que querías (Agrupado por Cripto y Año)
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

    # FIFO Detallado
    fifo_detallado.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)

    # Instrucciones
    pd.DataFrame({
        'Concepto': ['Ganancia/Pérdida Patrimonial', 'Referral Commission'],
        'Dónde declararlo': ['Casillas 1800-1814 (F2)', 'Casilla 0304 (Base General)'],
        'Notas': ['Usa la pestaña "Agrupado por Año"', 'No entra en Base del Ahorro']
    }).to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print(f"✅ Archivo generado: **{archivo_salida}**")
print("   → Pestaña 'Agrupado por Año' lista para declarar (por cripto + año)")
print("   → Incluye columna 'Tipo Contraprestación'")