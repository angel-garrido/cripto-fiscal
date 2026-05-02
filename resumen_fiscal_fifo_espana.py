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

# Referral (Se declaran en Base General, no son Ganancia Patrimonial de ahorro)
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False)

# ====================== PREPARACIÓN FIFO ======================
# Entradas: Compras + Recompensas/Minería (estos últimos tienen coste de adquisición = valor al recibirlos)
entradas = df[df['tipo'].isin(['compra', 'recompensa', 'minería']) & ~df['es_referral']].copy()
entradas['valor_unitario'] = entradas['total eur (tras pagar comisión)'].fillna(0) / entradas['cantidad'].replace(0, 1)

# Ventas: Solo nos interesan salidas de Cripto (no de EUR)
ventas_ganancia = df[(df['tipo'] == 'venta') & (df['moneda'] != 'EUR') & ~df['es_referral']].copy()

def calcular_fifo_estricto(ventas_df, entradas_df):
    inventario = []
    detalle = []

    # Cargar inventario ordenado por fecha
    for _, e in entradas_df.sort_values('fecha').iterrows():
        inventario.append({
            'fecha': e['fecha'],
            'moneda': e['moneda'],
            'cantidad': float(e['cantidad']),
            'valor_unitario': float(e['valor_unitario'])
        })

    for _, venta in ventas_df.sort_values('fecha').iterrows():
        cant_pendiente = float(venta['cantidad'])
        total_ingreso_venta = float(venta['total eur (tras pagar comisión)'])
        moneda = venta['moneda']
        fecha = venta['fecha']
        
        # Precio medio de venta para esta transacción
        precio_venta_unitario = total_ingreso_venta / cant_pendiente if cant_pendiente > 0 else 0

        # Buscar en inventario (FIFO)
        i = 0
        while cant_pendiente > 1e-9 and i < len(inventario):
            item = inventario[i]
            
            if item['moneda'] == moneda and item['fecha'] <= fecha:
                usado = min(cant_pendiente, item['cantidad'])
                
                coste_adquisicion = usado * item['valor_unitario']
                valor_transmision = usado * precio_venta_unitario
                beneficio = valor_transmision - coste_adquisicion

                detalle.append({
                    'Año': fecha.year,
                    'Fecha Venta': fecha,
                    'Moneda': moneda,
                    'Cantidad Vendida': usado, # <--- CORREGIDO: Usamos la parte proporcional
                    'Valor Transmisión': round(valor_transmision, 4),
                    'Valor Adquisición': round(coste_adquisicion, 4),
                    'Beneficio/Pérdida': round(beneficio, 4)
                })

                item['cantidad'] -= usado
                cant_pendiente -= usado
                
                if item['cantidad'] <= 1e-9:
                    inventario.pop(i)
                    continue # No incrementamos i porque el siguiente elemento ahora es el i
            i += 1
            
    return pd.DataFrame(detalle)

fifo_detalle = calcular_fifo_estricto(ventas_ganancia, entradas)

# ====================== REPORTES ======================
# 1. Resumen Anual (Totales por categoría)
años = sorted(df['año'].unique())
resumen_anual = pd.DataFrame({'Año': años}).set_index('Año')

resumen_anual['Ganancia_Patrimonial'] = fifo_detalle.groupby('Año')['Beneficio/Pérdida'].sum()
resumen_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
resumen_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()

resumen_anual = resumen_anual.fillna(0).round(2).reset_index()

# 2. Agrupado por Año y Moneda (Para casillas 1800+)
agrupado = fifo_detalle.groupby(['Año', 'Moneda']).agg({
    'Cantidad Vendida': 'sum',
    'Valor Transmisión': 'sum',
    'Valor Adquisición': 'sum',
    'Beneficio/Pérdida': 'sum'
}).round(2).reset_index()

# Exportar
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)
    fifo_detalle.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)
    
    pd.DataFrame({
        'Concepto': ["Ganancia/Pérdida Patrimonial", "Rendimientos Recompensas", "Rendimientos Minería", "Referral Commission"],
        'Casilla Renta (Aprox)': ["1800-1814 (Base Ahorro)", "0033 (Base Ahorro)", "0033 (Actividad Econ./Ahorro)", "0304 (Base General)"],
        'Descripción': ["Venta o permuta de criptos", "Earn, Staking, etc.", "Minería profesional", "Amigos/Afiliados"]
    }).to_excel(writer, sheet_name="Ayuda Fiscal", index=False)

print("✅ Proceso completado. Revisa 'Agrupado por Año' para los valores de la Renta.")
# Exportar con instrucciones detalladas
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    resumen_anual.to_excel(writer, sheet_name="Resumen Anual", index=False)
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)
    fifo_detalle.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)
    
    # Pestaña de Instrucciones Mejorada
    instrucciones_data = [
        {
            'Concepto': "Ganancia/Pérdida Patrimonial (Ventas y Permutas)",
            'Casillas Renta': "1800 a 1814",
            'Dónde mirar en este Excel': "Pestaña 'Agrupado por Año'",
            'Nota Fiscal España': "Cada permuta (cambio de una cripto por otra) cuenta como una venta. Se debe declarar el beneficio en la base del ahorro."
        },
        {
            'Concepto': "Rendimientos de Recompensas (Staking / Earn)",
            'Casillas Renta': "0033",
            'Dónde mirar en este Excel': "Pestaña 'Resumen Anual' -> Rendimientos_Recompensas",
            'Nota Fiscal España': "Se declaran por el valor en EUR en el momento de la recepción como Rendimientos del Capital Mobiliario."
        },
        {
            'Concepto': "Rendimientos de Minería",
            'Casillas Renta': "0033 / Actividad Económica",
            'Dónde mirar en este Excel': "Pestaña 'Resumen Anual' -> Rendimientos_Minería",
            'Nota Fiscal España': "Si no es actividad profesional, suele ir a la 0033. Si es profesional, requiere alta en IAE y autónomos."
        },
        {
            'Concepto': "Comisiones de Referidos (Referral)",
            'Casillas Renta': "0304",
            'Dónde mirar en este Excel': "Pestaña 'Resumen Anual' -> Referral_Commission",
            'Nota Fiscal España': "Tributan en la Base General como 'Otros ingresos', no en la del ahorro."
        }
    ]
    
    df_instrucciones = pd.DataFrame(instrucciones_data)
    df_instrucciones.to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print("✅ Archivo generado con instrucciones fiscales detalladas.")
