import pandas as pd

archivo_entrada = "Cripto_Control_Fiscal.xlsx"
archivo_salida = "resumen_fiscal_crypto_ESPANA.xlsx"

# Cargar datos directamente del archivo proporcionado
df = pd.read_excel(archivo_entrada, sheet_name="Transacciones")

# --- NORMALIZACIÓN ---
df.columns = df.columns.str.strip().str.lower()
df['tipo'] = df['tipo'].str.strip().str.lower()
df['moneda'] = df['moneda'].str.strip().str.upper()
df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
df['año'] = df['fecha'].dt.year

# Identificar Referidos (tributan en Base General)
df['es_referral'] = df['tipo'].str.contains('referral', case=False, na=False)

# --- PREPARACIÓN FIFO ---
# Las Recompensas/Minería entran al inventario con su valor en EUR en el momento de recibirlas
entradas = df[df['tipo'].isin(['compra', 'recompensa', 'minería']) & ~df['es_referral']].copy()
entradas['valor_unitario'] = entradas['total eur (tras pagar comisión)'].fillna(0) / entradas['cantidad'].replace(0, 1)

# Filtramos solo ventas de criptoactivos (excluimos EUR)
ventas_ganancia = df[(df['tipo'] == 'venta') & (df['moneda'] != 'EUR') & ~df['es_referral']].copy()

def calcular_fifo_estricto(ventas_df, entradas_df, df_full):
    inventario = []
    detalle = []

    # Cargar inventario ordenado cronológicamente
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
        
        # --- DETERMINACIÓN DEL TIPO DE CONTRAPRESTACIÓN (Letra Renta) ---
        # Si en la misma fecha hay una 'compra' de una cripto (no EUR), es una PERMUTA
        vinculadas = df_full[df_full['fecha'] == fecha]
        tiene_compra_cripto = vinculadas[(vinculadas['tipo'] == 'compra') & (vinculadas['moneda'] != 'EUR')].any().any()
        
        # N: Otra moneda virtual (Permutas) | F: Moneda curso legal (Venta a €)
        tipo_contraprestacion = 'N' if tiene_compra_cripto else 'F'
        
        precio_venta_unitario = total_ingreso_venta / cant_pendiente if cant_pendiente > 0 else 0

        i = 0
        while cant_pendiente > 1e-9 and i < len(inventario):
            item = inventario[i]
            if item['moneda'] == moneda and item['fecha'] <= fecha:
                usado = min(cant_pendiente, item['cantidad'])
                coste_adq = usado * item['valor_unitario']
                val_trans = usado * precio_venta_unitario

                detalle.append({
                    'Año': fecha.year,
                    'Fecha Venta': fecha,
                    'Moneda': moneda,
                    'Cantidad Vendida': usado,
                    'Valor Transmisión': round(val_trans, 4),
                    'Valor Adquisición': round(coste_adq, 4),
                    'Beneficio/Pérdida': round(val_trans - coste_adq, 4),
                    'Tipo Contraprestación': tipo_contraprestacion
                })

                item['cantidad'] -= usado
                cant_pendiente -= usado
                if item['cantidad'] <= 1e-9:
                    inventario.pop(i)
                    continue 
            i += 1
            
    return pd.DataFrame(detalle)

# Ejecución del motor FIFO
fifo_detalle = calcular_fifo_estricto(ventas_ganancia, entradas, df)

# --- GENERACIÓN DE EXCEL ---
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    # 1. Resumen Anual por categorías de Renta
    res_anual = pd.DataFrame({'Año': sorted(df['año'].unique())}).set_index('Año')
    res_anual['Ganancia_Patrimonial'] = fifo_detalle.groupby('Año')['Beneficio/Pérdida'].sum()
    res_anual['Rendimientos_Recompensas'] = df[(df['tipo']=='recompensa') & ~df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
    res_anual['Rendimientos_Minería'] = df[df['tipo']=='minería'].groupby('año')['total eur (tras pagar comisión)'].sum()
    res_anual['Referral_Commission'] = df[df['es_referral']].groupby('año')['total eur (tras pagar comisión)'].sum()
    res_anual.fillna(0).round(2).reset_index().to_excel(writer, sheet_name="Resumen Anual", index=False)

    # 2. Agrupado por Año y Moneda (Clave para las casillas 1800+)
    agrupado = fifo_detalle.groupby(['Año', 'Moneda', 'Tipo Contraprestación']).agg({
        'Cantidad Vendida': 'sum',
        'Valor Transmisión': 'sum',
        'Valor Adquisición': 'sum',
        'Beneficio/Pérdida': 'sum'
    }).round(2).reset_index()
    agrupado.to_excel(writer, sheet_name="Agrupado por Año", index=False)

    # 3. Detalle completo de cada tramo FIFO
    fifo_detalle.to_excel(writer, sheet_name="FIFO Tracking Detallado", index=False)
    
    # 4. Leyenda Técnica Renta
    pd.DataFrame([
        {'Clave': 'N', 'Descripción': 'Intercambio de criptos o stablecoins (Permuta)', 'Ejemplo': 'USDC por BTC'},
        {'Clave': 'F', 'Descripción': 'Venta por Euros (Moneda de curso legal)', 'Ejemplo': 'BTC por Euros'}
    ]).to_excel(writer, sheet_name="Ayuda Letras Contraprestación", index=False)

print(f"✅ Análisis completado. Se ha generado '{archivo_salida}'.")
