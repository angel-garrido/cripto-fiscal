import pandas as pd

archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA.xlsx"

# Cargar datos
df = pd.read_excel(archivo_entrada, sheet_name="Transacciones")

# --- NORMALIZACIÓN ---
df.columns = df.columns.str.strip().str.lower()
df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
df['año'] = df['fecha'].dt.year

# Identificar Referidos (tributan en Base General, casilla 0304)
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False)

# --- PREPARACIÓN FIFO ---
# Entradas: Compras, Recompensas y Minería (estas últimas entran con coste = valor al recibirlas)
entradas = df[df['tipo'].isin(['compra', 'recompensa', 'minería']) & ~df['es_referral']].copy()
entradas['valor_unitario'] = entradas['total eur (tras pagar comisión)'].fillna(0) / entradas['cantidad'].replace(0, 1)

# Ventas: Solo salidas de Cripto (excluimos ventas de EUR a cambio de Cripto)
ventas_ganancia = df[(df['tipo'] == 'venta') & (df['moneda'] != 'EUR') & ~df['es_referral']].copy()

def calcular_fifo_estricto(ventas_df, entradas_df, df_full):
    inventario = []
    detalle = []

    # Inicializar inventario ordenado por fecha
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
        
        # Lógica de Tipo de Operación para la Renta (Venta vs Permuta)
        # Si en el mismo momento hubo una compra de otra cripto, es Permuta (P), si no, Venta (V)
        vinculadas = df_full[df_full['fecha'] == fecha]
        es_permuta = vinculadas[(vinculadas['tipo'] == 'compra') & (vinculadas['moneda'] != 'EUR')].any().any()
        tipo_irpf = 'P' if es_permuta else 'V'
        
        precio_venta_unitario = total_ingreso_venta / cant_pendiente if cant_pendiente > 0 else 0

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
                    'Cantidad Vendida': usado, # Se registra la parte proporcional usada
                    'Valor Transmisión': round(valor_transmision, 4),
                    'Valor Adquisición': round(coste_adquisicion, 4),
                    'Beneficio/Pérdida': round(beneficio, 4),
                    'Tipo Operación': tipo_irpf
                })

                item['cantidad'] -= usado
                cant_pendiente -= usado
                
                if item['cantidad'] <= 1e-9:
                    inventario.pop(i)
                    continue 
            i += 1
            
    return pd.DataFrame(detalle)

# Ejecutar cálculo
fifo_detalle = calcular_fifo_estricto(ventas_ganancia, entradas, df)

# --- REPORTES Y EXPORTACIÓN ---
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    # 1. Resumen Anual (Totales por categoría)
    resumen_anual = pd.DataFrame({'Año': sorted(df['año'].unique())}).set_index('Año')
    resumen_anual['Ganancia_Patrimonial'] = fifo_detalle.groupby('Año')['Beneficio/Pérdida'].sum()
    resumen_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
    resumen_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
    resumen_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
    resumen_anual.fillna(0).round(2).reset_index().to_excel(writer, sheet_name="Resumen Anual", index=False)

    # 2. Agrupado por Año y Moneda (Modelo Renta casillas 1800+)
    agrupado = fifo_detalle.groupby(['Año', 'Moneda', 'Tipo Operación']).agg({
        'Cantidad Vendida': 'sum',
        'Valor Transmisión': 'sum',
        'Valor Adquisición': 'sum',
        'Beneficio/Pérdida': 'sum'
    }).round(2).reset_index()
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

    # 3. FIFO Detallado (Trazabilidad)
    fifo_detalle.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)

    # 4. Instrucciones Fiscales España
    pd.DataFrame([
        {'Concepto': "Ganancia/Pérdida Patrimonial", 'Casillas': "1800-1814", 'Origen': "Ventas y Permutas (Agrupado por Año)", 'Base': "Ahorro"},
        {'Concepto': "Rendimientos Staking/Earn", 'Casillas': "0033", 'Origen': "Rendimientos_Recompensas", 'Base': "Ahorro"},
        {'Concepto': "Referidos", 'Casillas': "0304", 'Origen': "Referral_Commission", 'Base': "General"}
    ]).to_excel(writer, sheet_name="Instrucciones Renta España", index=False)

print(f"✅ Archivo '{archivo_salida}' generado correctamente.")
