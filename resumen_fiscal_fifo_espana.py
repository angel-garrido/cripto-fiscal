import pandas as pd
from google.colab import files

# ====================== CONFIGURACIÓN ======================
archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA.xlsx"

# Leer datos
df = pd.read_excel(archivo_entrada, sheet_name="Transacciones")

# Normalizar columnas
df.columns = df.columns.str.strip().str.lower()
df.rename(columns={
    'precio de la cripto en eur': 'precio_unitario',
    'total eur (tras pagar comisión)': 'total_eur',
    'valoración fiscal (€)': 'valoracion_fiscal'
}, inplace=True)

# Normalizar texto y fechas
df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
df = df.sort_values('fecha').reset_index(drop=True)

# ====================== ENTRADAS PARA FIFO ======================
entradas = df[df['tipo'].isin(['compra', 'recompensa', 'minería'])].copy()

# Valor de adquisición correcto (España)
entradas['valor_total'] = entradas['total_eur'].fillna(0)

mask_earn = entradas['tipo'].isin(['recompensa', 'minería'])
entradas.loc[mask_earn, 'valor_total'] = (
    entradas.loc[mask_earn, 'precio_unitario'] * entradas.loc[mask_earn, 'cantidad']
)

entradas['valor_unitario'] = entradas['valor_total'] / entradas['cantidad'].replace(0, 1)

# ====================== VENTAS ======================
ventas = df[df['tipo'] == 'venta'].copy()
ventas_ganancia = ventas[~ventas['moneda'].isin(['EUR'])].copy()   # Opción conservadora

# ====================== FUNCIÓN FIFO ======================
def calcular_fifo_espana(ventas_df, entradas_df):
    inventario = []
    detalle = []
    resumen = []

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

        while restante > 0 and inventario:
            posibles = [item for item in inventario 
                       if item['moneda'] == moneda and item['fecha'] <= fecha_venta]
            if not posibles:
                break

            entrada = sorted(posibles, key=lambda x: x['fecha'])[0]
            usado = min(restante, entrada['cantidad'])

            coste = usado * entrada['valor_unitario']
            ingreso = (usado / cantidad_total) * total_transmision if cantidad_total > 0 else 0
            beneficio = ingreso - coste
            beneficio_total += beneficio

            detalle.append({
                'Fecha Venta': fecha_venta,
                'Moneda': moneda,
                'Cantidad Vendida': round(cantidad_total, 8),
                'Precio Venta Unitario': round(venta.get('precio_unitario', 0), 4),
                'Fecha Adquisición': entrada['fecha'],
                'Tipo Adquisición': entrada['tipo'],
                'Cantidad Usada': round(usado, 8),
                'Precio Adquisición Unitario': round(entrada['precio_unitario'], 4),
                'Coste Adquisición': round(coste, 2),
                'Ingreso Transmisión': round(ingreso, 2),
                'Beneficio/Pérdida': round(beneficio, 2)
            })

            entrada['cantidad'] -= usado
            if entrada['cantidad'] <= 1e-8:
                inventario.remove(entrada)
            restante -= usado

        resumen.append({
            'fecha_venta': fecha_venta,
            'moneda': moneda,
            'beneficio': round(beneficio_total, 2)
        })

    return pd.DataFrame(resumen), pd.DataFrame(detalle)


# Ejecutar
resumen_fifo, fifo_tracking = calcular_fifo_espana(ventas_ganancia, entradas)

resumen_fifo['Año'] = resumen_fifo['fecha_venta'].dt.year
fifo_tracking['Año'] = pd.to_datetime(fifo_tracking['Fecha Venta']).dt.year

# Resumen anual
df['año'] = df['fecha'].dt.year

recompensas_anual = df[df['tipo'] == 'recompensa'].groupby('año')['valor_total'].sum().rename("Rendimientos_Recompensas")
mineria_anual = df[df['tipo'] == 'minería'].groupby('año')['valor_total'].sum().rename("Rendimientos_Minería")
ganancias_patrimoniales = resumen_fifo.groupby('Año')['beneficio'].sum().rename("Ganancia_Patrimonial")

resumen_anual = pd.concat([ganancias_patrimoniales, recompensas_anual, mineria_anual], axis=1).fillna(0).reset_index()

# ====================== EXPORTAR ======================
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)
    resumen_fifo.to_excel(writer, sheet_name="Ganancias FIFO", index=False)
    fifo_tracking.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)

    # Hoja de instrucciones
    instrucciones = pd.DataFrame({
        'Concepto': [
            "Ganancia/Pérdida Patrimonial (Ventas y permutas)",
            "Rendimientos del Capital - Recompensas (Earn, Staking...)",
            "Rendimientos del Capital - Minería",
            "Total aproximado Base del Ahorro"
        ],
        'Dónde declararlo (Renta 2025)': [
            "Casillas 1800 - 1814",
            "Casilla 0031",
            "Casilla 0031",
            "Suma de los anteriores"
        ],
        'Notas': [
            "Calculado con FIFO. Incluye swaps cripto-cripto.",
            "Valor de mercado al recibir la recompensa.",
            "Valor de mercado al recibir.",
            "Guarda este archivo como justificante."
        ]
    })
    instrucciones.to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print(f"✅ Archivo generado correctamente: **{archivo_salida}**")
files.download(archivo_salida)