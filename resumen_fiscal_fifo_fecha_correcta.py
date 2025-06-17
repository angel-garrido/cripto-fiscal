import pandas as pd

# Leer archivo
archivo = "Cripto_Control_Fiscal.xlsx"
df = pd.read_excel(archivo, sheet_name="Transacciones")

# Normalizar columnas
df.columns = df.columns.str.strip().str.lower()
df.rename(columns={
    'precio de la cripto en eur': 'precio_unitario',
    'total eur (tras pagar comisión)': 'total_eur',
    'valoración fiscal (€)': 'valoracion_fiscal'
}, inplace=True)

# Convertir fecha correctamente (con hora)
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True)
df = df.sort_values('fecha')

# Entradas válidas al inventario FIFO
entradas = df[df['tipo'].str.lower().isin(['compra', 'recompensa', 'minería'])].copy()
entradas['valor_total'] = entradas['total_eur'].fillna(entradas['valoracion_fiscal'])
entradas['valor_unitario'] = entradas['valor_total'] / entradas['cantidad']

# Ventas
ventas = df[df['tipo'].str.lower() == 'venta'].copy()

# FIFO tracking con validación temporal
def calcular_fifo_con_fecha(ventas, entradas):
    inventario = []
    resumen = []
    detalle = []

    for _, e in entradas.iterrows():
        inventario.append({
            'fecha': e['fecha'],
            'moneda': e['moneda'],
            'cantidad': e['cantidad'],
            'valor_unitario': e['valor_unitario'],
            'tipo': e['tipo']
        })

    for _, venta in ventas.iterrows():
        cantidad = venta['cantidad']
        total_venta = venta['total_eur']
        moneda = venta['moneda']
        fecha_venta = venta['fecha']
        precio_venta_unitario = total_venta / cantidad
        restante = cantidad
        beneficio_total = 0

        while restante > 0:
            posibles = [i for i in inventario if i['moneda'] == moneda and i['fecha'] <= fecha_venta]
            if not posibles:
                break  # No hay entradas válidas

            entrada = sorted(posibles, key=lambda x: x['fecha'])[0]
            usado = min(restante, entrada['cantidad'])
            coste = usado * entrada['valor_unitario']
            ingreso = usado * precio_venta_unitario
            beneficio = ingreso - coste
            beneficio_total += beneficio

            detalle.append({
                'fecha_venta': fecha_venta,
                'moneda': moneda,
                'cantidad_vendida': cantidad,
                'precio_venta_unitario': round(precio_venta_unitario, 2),
                'fecha_origen': entrada['fecha'],
                'tipo_origen': entrada['tipo'],
                'cantidad_usada': usado,
                'precio_compra_unitario': round(entrada['valor_unitario'], 2),
                'coste': round(coste, 2),
                'ingreso': round(ingreso, 2),
                'beneficio': round(beneficio, 2)
            })

            entrada['cantidad'] -= usado
            if entrada['cantidad'] == 0:
                inventario.remove(entrada)
            restante -= usado

        resumen.append({
            'fecha_venta': fecha_venta,
            'moneda': moneda,
            'beneficio': round(beneficio_total, 2)
        })

    return pd.DataFrame(resumen), pd.DataFrame(detalle)

# Ejecutar FIFO corregido
resumen_fifo, fifo_tracking = calcular_fifo_con_fecha(ventas, entradas)
resumen_fifo['año'] = resumen_fifo['fecha_venta'].dt.year

# Calcular resumen anual
df['año'] = df['fecha'].dt.year
recompensas = df[df['tipo'].str.lower() == 'recompensa']
mineria = df[df['tipo'].str.lower() == 'minería']

recompensas_anual = recompensas.groupby('año')['valoracion_fiscal'].sum().rename("recompensas")
mineria_anual = mineria.groupby('año')['valoracion_fiscal'].sum().rename("mineria")
ventas_anual = resumen_fifo.groupby('año')['beneficio'].sum().rename("beneficio_ventas")

resumen = pd.concat([ventas_anual, recompensas_anual, mineria_anual], axis=1).fillna(0).reset_index()

# Guardar a Excel
with pd.ExcelWriter("resumen_fiscal_crypto.xlsx", engine='openpyxl') as writer:
    resumen.to_excel(writer, sheet_name="Resumen Anual", index=False)
    resumen_fifo.to_excel(writer, sheet_name="Ganancias FIFO", index=False)
    fifo_tracking.to_excel(writer, sheet_name="FIFO Tracking", index=False)

print("✅ Resumen fiscal generado correctamente: resumen_fiscal_crypto.xlsx")
