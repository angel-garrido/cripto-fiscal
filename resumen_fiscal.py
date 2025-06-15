# resumen_fiscal.py

import pandas as pd

# Leer el archivo Excel
archivo = "Cripto_Control_Fiscal.xlsx"
df = pd.read_excel(archivo)

# Normalizar nombres de columnas
df.columns = df.columns.str.strip().str.lower()
df.rename(columns={
    'precio de la cripto en eur': 'precio_unitario_eur',
    'total eur (tras pagar comisión)': 'total_eur',
    'comisión eur': 'comision'
}, inplace=True)

# Convertir fecha y ordenar
df['fecha'] = pd.to_datetime(df['fecha'])
df = df.sort_values('fecha')

# Clasificar por tipo de movimiento
ventas = df[df['tipo'].str.lower() == 'venta'].copy()
compras = df[df['tipo'].str.lower() == 'compra'].copy()
recompensas = df[df['tipo'].str.lower() == 'recompensa'].copy()
mineria = df[df['tipo'].str.lower() == 'minería'].copy()
comisiones = df[df['comision'] > 0].copy()

# Función FIFO
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

# Calcular resumen anual
recompensas.loc[:, 'año'] = recompensas['fecha'].dt.year
mineria.loc[:, 'año'] = mineria['fecha'].dt.year
comisiones.loc[:, 'año'] = comisiones['fecha'].dt.year

ventas_anual = resultados_fifo.groupby('año')['beneficio'].sum().rename("beneficio_ventas")
recompensas_anual = recompensas.groupby('año')['total_eur'].sum().rename("recompensas")
mineria_anual = mineria.groupby('año')['total_eur'].sum().rename("mineria")
comisiones_anual = comisiones.groupby('año')['comision'].sum().rename("comisiones")

# Unir todos los datos
resumen = pd.concat([
    ventas_anual,
    recompensas_anual,
    mineria_anual,
    comisiones_anual
], axis=1).fillna(0).reset_index()

# Guardar el resumen a Excel
resumen.to_excel("resumen_fiscal_crypto.xlsx", index=False)

print("✅ Resumen fiscal generado correctamente: resumen_fiscal_crypto.xlsx")
