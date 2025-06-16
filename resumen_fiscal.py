# resumen_fiscal_final.py
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

# Asegurar tipos y ordenar
df['fecha'] = pd.to_datetime(df['fecha'])
df = df.sort_values('fecha')

# Entradas al inventario FIFO: compra, recompensa, minería
entradas = df[df['tipo'].str.lower().isin(['compra', 'recompensa', 'minería'])].copy()
entradas['valor_total'] = entradas['total_eur'].fillna(entradas['valoracion_fiscal'])
entradas['valor_unitario'] = entradas['valor_total'] / entradas['cantidad']

# Ventas
ventas = df[df['tipo'].str.lower() == 'venta'].copy()

# FIFO
def calcular_fifo(ventas, entradas):
    inventario = []
    resultados = []

    for _, e in entradas.iterrows():
        inventario.append({
            'fecha': e['fecha'],
            'moneda': e['moneda'],
            'cantidad': e['cantidad'],
            'valor_unitario': e['valor_unitario']
        })

    for _, venta in ventas.iterrows():
        cantidad = venta['cantidad']
        total_venta = venta['total_eur']
        moneda = venta['moneda']
        restante = cantidad
        beneficio_total = 0

        while restante > 0 and any(i['moneda'] == moneda for i in inventario):
            entrada = next(i for i in inventario if i['moneda'] == moneda)
            usado = min(restante, entrada['cantidad'])
            coste = usado * entrada['valor_unitario']
            ingreso = (usado / cantidad) * total_venta
            beneficio = ingreso - coste
            beneficio_total += beneficio

            entrada['cantidad'] -= usado
            if entrada['cantidad'] == 0:
                inventario.remove(entrada)

            restante -= usado

        resultados.append({
            'fecha_venta': venta['fecha'],
            'moneda': moneda,
            'beneficio': round(beneficio_total, 2)
        })

    return pd.DataFrame(resultados)

# Ejecutar FIFO
fifo_resultado = calcular_fifo(ventas, entradas)
fifo_resultado['año'] = fifo_resultado['fecha_venta'].dt.year

# Resumen anual
df['año'] = df['fecha'].dt.year
recompensas = df[df['tipo'].str.lower() == 'recompensa']
mineria = df[df['tipo'].str.lower() == 'minería']

recompensas_anual = recompensas.groupby('año')['valoracion_fiscal'].sum().rename("recompensas")
mineria_anual = mineria.groupby('año')['valoracion_fiscal'].sum().rename("mineria")
ventas_anual = fifo_resultado.groupby('año')['beneficio'].sum().rename("beneficio_ventas")

resumen = pd.concat([ventas_anual, recompensas_anual, mineria_anual], axis=1).fillna(0).reset_index()

# Exportar
resumen.to_excel("resumen_fiscal_crypto.xlsx", index=False)
print("✅ Resumen fiscal generado correctamente: resumen_fiscal_crypto.xlsx")
