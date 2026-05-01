import pandas as pd
from google.colab import files
from datetime import datetime

# ====================== CONFIGURACIÓN ======================
archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA_PRECISO.xlsx"

# Leer datos
df = pd.read_excel(archivo_entrada, sheet_name="Transacciones")

# Normalizar
df.columns = df.columns.str.strip().str.lower()
df.rename(columns={
    'precio de la cripto en eur': 'precio_unitario',
    'total eur (tras pagar comisión)': 'total_eur',
    'valoración fiscal (€)': 'valoracion_fiscal'
}, inplace=True)

df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
df = df.sort_values('fecha').reset_index(drop=True)

# ====================== VALOR_TOTAL ======================
df['valor_total'] = df['total_eur'].fillna(0)
mask_earn = df['tipo'].isin(['recompensa', 'minería'])
df.loc[mask_earn, 'valor_total'] = df.loc[mask_earn, 'precio_unitario'] * df.loc[mask_earn, 'cantidad']

# ====================== ENTRADAS PARA FIFO ======================
entradas = df[df['tipo'].isin(['compra', 'recompensa', 'minería'])].copy()
entradas['valor_unitario'] = entradas['valor_total'] / entradas['cantidad'].replace(0, 1)

# ====================== VENTAS - VERSIÓN MÁS PRECISA ======================
ventas = df[df['tipo'] == 'venta'].copy()

# Excluir ventas de EUR y filtrar solo operaciones relevantes para 2025
ventas_ganancia = ventas[
    ~ventas['moneda'].isin(['EUR']) & 
    (ventas['fecha'].dt.year >= 2020)
].copy()

print(f"Total ventas procesadas para Ganancia Patrimonial: {len(ventas_ganancia)}")

# ====================== FUNCIÓN FIFO MEJORADA ======================
def calcular_fifo_preciso(ventas_df, entradas_df):
    inventario = []
    detalle = []
    resumen = []

    # Cargar inventario
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
        beneficio_total = 0.0
        usados = []

        while restante > 0 and inventario:
            posibles = [item for item in inventario 
                       if item['moneda'] == moneda and item['fecha'] <= fecha_venta]
            if not posibles:
                break

            # FIFO: entrada más antigua
            entrada = sorted(posibles, key=lambda x: x['fecha'])[0]
            usado = min(restante, entrada['cantidad'])

            coste = usado * entrada['valor_unitario']
            ingreso = (usado / cantidad_total) * total_transmision if cantidad_total > 0 else 0
            beneficio = ingreso - coste
            beneficio_total += beneficio

            usados.append({
                'fecha_origen': entrada['fecha'],
                'tipo_origen': entrada['tipo'],
                'cantidad_usada': round(usado, 8),
                'coste': round(coste, 2),
                'ingreso': round(ingreso, 2),
                'beneficio': round(beneficio, 2)
            })

            entrada['cantidad'] -= usado
            if entrada['cantidad'] <= 1e-8:
                inventario.remove(entrada)
            restante -= usado

        resumen.append({
            'fecha_venta': fecha_venta,
            'moneda': moneda,
            'beneficio': round(beneficio_total, 2),
            'cantidad_vendida': round(cantidad_total, 8)
        })

        # Guardar detalle
        for u in usados:
            detalle.append({
                'Fecha Venta': fecha_venta,
                'Moneda': moneda,
                'Cantidad Vendida': round(cantidad_total, 8),
                'Fecha Adquisición': u['fecha_origen'],
                'Tipo Adquisición': u['tipo_origen'],
                'Cantidad Usada': u['cantidad_usada'],
                'Coste Adquisición': u['coste'],
                'Ingreso Transmisión': u['ingreso'],
                'Beneficio/Pérdida': u['beneficio']
            })

    return pd.DataFrame(resumen), pd.DataFrame(detalle)


# Ejecutar
resumen_fifo, fifo_tracking = calcular_fifo_preciso(ventas_ganancia, entradas)

resumen_fifo['Año'] = resumen_fifo['fecha_venta'].dt.year

# ====================== RESUMEN ANUAL ======================
df['año'] = df['fecha'].dt.year

recompensas_anual = df[df['tipo'] == 'recompensa'].groupby('año')['valor_total'].sum().rename("Rendimientos_Recompensas")
mineria_anual = df[df['tipo'] == 'minería'].groupby('año')['valor_total'].sum().rename("Rendimientos_Minería")
ganancias_patrimoniales = resumen_fifo.groupby('Año')['beneficio'].sum().rename("Ganancia_Patrimonial")

resumen_anual = pd.concat([ganancias_patrimoniales, recompensas_anual, mineria_anual], axis=1).fillna(0).reset_index()

print("\n=== RESUMEN FINAL 2025 ===")
print(resumen_anual[resumen_anual['Año'] == 2025])

# ====================== EXPORTAR ======================
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)
    resumen_fifo.to_excel(writer, sheet_name="Ganancias FIFO", index=False)
    fifo_tracking.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)

    instrucciones = pd.DataFrame({
        'Concepto': ["Ganancia/Pérdida Patrimonial", "Rendimientos Recompensas", "Rendimientos Minería"],
        'Dónde declararlo': ["Casillas 1800-1814", "Casilla 0033", "Casilla 0033"],
        'Notas': ["Método FIFO mejorado", "Valor de mercado", "Valor de mercado"]
    })
    instrucciones.to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print(f"\n✅ Archivo generado: **{archivo_salida}**")
files.download(archivo_salida)