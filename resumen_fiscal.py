# resumen_fiscal.py

import pandas as pd

# Leer el archivo Excel
archivo = "Cripto_Control_Fiscal.xlsx"
df = pd.read_excel(archivo)

# Normalizar columnas
df.columns = df.columns.str.strip().str.lower()
df.rename(columns={
    'precio de la cripto en eur': 'precio_unitario_eur',
    'total eur (tras pagar comisión)': 'total_eur',
    'comisión': 'comision'
}, inplace=True)

# Convertir fecha y ordenar
df['fecha'] = pd.to_datetime(df['fecha'])
df = df.sort_values('fecha')

# Clasificar movimientos
ventas = df[df['tipo'].str.lower() == 'venta']
compras = df[df['tipo'].str.lower() == 'compra']
recompensas = df[df['tipo'].str.lower() == 'recompensa']
mineria = df[df['tipo'].str.lower() == 'minería']
comisiones = df[df['comision'] > 0]

# FIFO
def calcular_fifo(ventas, compras):
    inventario = []
    resultados = []

    for _, compra in compras.iterrows():
        inventario.append({
            'fecha': compra['fecha'],
            'moneda': compra['moneda'],
            'cantidad': compra['cantidad'],
            'valor_unitario': compra['total_eur'] / compra['cantidad'],
        })

    for _, venta in ventas.iterrows():
        cantidad_vendida = venta['cantidad']
        total_venta_eur = venta['total_eur']
        moneda = venta['moneda']
        restante = cantidad_vendida
        beneficio_total = 0

        while restante > 0 and inventario:
            entrada = inventario[0]
            if entrada['moneda'] != moneda:
                inventario.pop(0)
                continue

            usado = min(restante, entrada['cantidad'])
            coste = usado * entrada['valor_unitario']
            ingreso = (usado / cantidad_vendida) * total_venta_eur
            beneficio = ingreso - coste
            beneficio_total += beneficio

            entrada['cantidad'] -= usado
            if entrada['cantidad'] == 0:
                inventario.pop(0)
            restante -= usado

        resultados.append({
            'fecha_venta': venta['fecha'],
            'moneda': moneda,
            'beneficio': beneficio_total
        })

    return pd.DataFrame(resultados)

# Ejecutar FIFO
resultados_fifo = calcular_fifo(ventas, compras)
resultados_fifo['año'] = resultados_fifo['fecha_venta'].dt.year

# Totales anuales
ventas_anual = resultados_fifo.groupby('año')['beneficio'].sum().rename("beneficio_ventas")
recompensas['año'] = recompensas['fecha'].dt.year
mineria['año'] = mineria['fecha'].dt.year
comisiones['año'] = comisiones['fecha'].dt.year

recompensas_anual = recompensas.groupby('año')['total_eur'].sum().rename("recompensas")
mineria_anual = mineria.groupby('año')['total_eur'].sum().rename("mineria")
comisiones_anual = comisiones.groupby('año')['comision'].sum().rename("comisiones")

# Combinar en resumen
resumen = pd.concat([
    ventas_anual, 
    recompensas_anual, 
    mineria_anual, 
    comisiones_anual
], axis=1).fillna(0).reset_index()

# Guardar a Excel
resumen.to_excel("resumen_fiscal_crypto.xlsx", index=False)

print("✅ Resumen fiscal generado: resumen_fiscal_crypto.xlsx")
