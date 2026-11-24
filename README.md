# BOTVENGANZA · Shorts simulados y longs por Executor

Aplicación web para Render que monitorea Binance USDT-M Futures y simula shorts por tramos cuando un símbolo supera niveles configurables de ganancia en 24 h.

## Estrategia implementada

- **Casi sin REST** (para evitar baneos de IP de Binance):
  - La única petición REST de datos es `exchangeInfo`, una vez al arrancar, y se guarda en disco. Si el caché tiene menos de `SYMBOL_REFRESH_HOURS`, el arranque no hace ninguna. Puede ir por un proxy (`REST_PROXY_URL`); si el proxy falla se reintenta una vez por la IP directa.
  - No se consulta ni se cambia el leverage: se usa el que ya tenga la cuenta.
  - La venganza conserva el modo de operación existente (`PAPER_MODE` y `LIVE_TRADING`) y sus señales al Executor.
- Precios, cambio 24h y volumen por WebSocket (`!ticker@arr`, `!markPrice@arr`, `<symbol>@bookTicker`).
- Velas de **todos** los símbolos solo por WebSocket (`kline_ws.py`), sin backfill REST:
  - **Una conexión por intervalo** (`KLINE_INTERVALS`, por defecto `1m`) con todos los símbolos suscritos (Binance admite 1024 streams por conexión).
  - Se guardan las últimas `KLINE_HISTORY` velas cerradas por símbolo en un buffer circular de numpy: 48 bytes por vela en `float64` (28 en `float32`), frente a ~300 bytes con objetos Python. Con 819 símbolos y 1500 velas son ~60 MB por intervalo (~35 MB en `float32`).
  - Las velas en formación se descartan sin parsear el JSON; solo se procesa el cierre.
  - La confirmación de entrada exige que la vela 1m del minuto anterior haya cerrado alcista.
- Los niveles del short son 50, 75, 100, 150, 200, 250 y 300 por defecto. Los notionals respectivos son 5, 5, 10, 20, 40, 80 y 160 USDT. El bot simula todos los tramos de esta estrategia.
- El LONG se abre en el fill 100 solo si el bot observa un cruce habilitado del cambio de 24 h desde menos de 100% hasta 100% o más. Si el short toma el fill 100 al desbloquearse con el símbolo ya en 200%, ese ciclo queda sin long. La señal al Executor usa el notional del fill 100. Si el short toma el 150 y el long sigue abierto, se agrega el notional de ese tramo al mismo long. Los totales por defecto son 10 USDT y 30 USDT.
- El SL del long usa por defecto el precio promedio de entrada del short simulado. Desde **Ajustes → Stop Loss de longs** puedes cambiarlo al precio de TP del short. Si el short cierra por SL con una pérdida de 8 USD o más y el long sigue abierto, el long cierra por TP. Para volver a abrirlo al mismo notional, el bot espera la respuesta satisfactoria del Executor a la señal de cierre. Si el short cierra antes, se liquida el long que siga asociado y termina ese ciclo.
- La reentrada tiene un TP de +10 USD medidos desde su propia entrada. El ajuste de SL también se aplica a la reentrada, con el precio promedio y el TP que tenía el short al cerrar.
- Cuando está habilitada, la venganza conserva su configuración, dirección y notional. Sus señales tienen un `trade_id` distinto del de la reentrada.
- El panel tiene tablas independientes para shorts y longs y muestra cuatro saldos de PnL, abierto y realizado por lado. Los importes se calculan con los precios que observa el bot.

## Modo de operación

Los shorts de la estrategia se simulan en todos los modos. La URL del Executor se define en `EXECUTOR_URL` o desde el panel. Con una URL configurada, las señales de longs y venganzas se envían incluso con `PAPER_MODE=true`. Con la URL vacía, el bot lleva sus cálculos de estrategia en local.

Las siguientes variables activan la vía directa a Binance que usa la venganza:

```bash
PAPER_MODE=false
LIVE_TRADING=true
BINANCE_API_KEY=tu_api_key
BINANCE_API_SECRET=tu_api_secret
```

## Variables de entorno principales

| Variable | Default | Descripción |
| --- | --- | --- |
| `PAPER_MODE` | `true` | Omite órdenes directas a Binance. |
| `LIVE_TRADING` | `false` | Habilita órdenes directas de venganza si también `PAPER_MODE=false`. |
| `EXECUTOR_URL` | URL del proyecto | Destino de las señales. Una cadena vacía desactiva el envío. |
| `ENTRY_LEVELS` | `50,75,100,150,200,250,300` | Niveles de subida 24h para abrir tramos. |
| `ENTRY_NOTIONALS` | `5,5,10,20,40,80,160` | USDT por tramo. |
| `TAKE_PROFIT_FRACTION` | `0.14284` | Ganancia objetivo del short sobre el notional total. |
| `SCAN_INTERVAL_SECONDS` | `60` | Frecuencia mínima de consulta REST para refrescar ganadores y actualizar la lista seguida por WebSocket. |
| `MAX_SYMBOLS` | `120` | Máximo de ganadores a evaluar por escaneo. |
| `MIN_GAIN_TO_SHOW` | `0` | Filtro mínimo de porcentaje para mostrar ganadores en la tabla. |
| `INCLUDE_SPOT_WINNERS` | `false` | Conservado solo para el fallback manual REST; el escaneo operativo usa futures por WebSocket. |
| `REST_PROXY_URL` | vacío | Proxy HTTP para la única petición REST del arranque (`exchangeInfo`). Configúralo como secreto. |
| `SYMBOL_REFRESH_HOURS` | `12` | Antigüedad máxima del caché de `exchangeInfo` en disco antes de volver a pedirlo al arrancar. |
| `KLINE_INTERVALS` | `1m` | Intervalos de velas, separados por comas. Cada uno es una conexión WebSocket. `1m` siempre se incluye. |
| `KLINE_HISTORY` | `2` | Velas cerradas guardadas por símbolo e intervalo. |
| `KLINE_DTYPE` | `float64` | `float32` usa la mitad de RAM a cambio de precisión. |
| `KLINE_WS_COMPRESSION` | `false` | Compresión deflate en las conexiones de velas (menos tráfico, más CPU). |
| `KLINE_ALLOW_NO_DATA` | `false` | Si es `true`, permite entrar cuando aún no hay vela cerrada reciente. |
| `KLINE_STREAMS_PER_CONN` | `1024` | Máximo de símbolos por conexión; solo si se supera se abre otra para el mismo intervalo. |
| `STATE_FILE` | `/tmp/botvenganza_state.json` | Último estado del panel, guardado en disco local. |
| `RECOVERY_FILE` | `/tmp/botvenganza_recovery.json` | Posiciones, ajustes, cooldowns e historial reciente para recuperar al arrancar. |
| `STATS_FILE` | `trade_stats.jsonl` | Archivo local con el historial de operaciones cerradas. |
| `SETTINGS_FILE` | `bot_settings.json` | Archivo local con los ajustes del panel. |

Usa rutas propias de este bot para estos archivos. En un hosting con disco efímero, configura las rutas en un volumen persistente si deben sobrevivir a un redeploy.

## Ejecutar local

```bash
pip install -r requirements.txt
python app.py
```

Abre `http://localhost:8000`.

## Diagnóstico de pantalla vacía

Si el bot abre posiciones en los logs pero la página no las muestra, revisa en la web el bloque **Estado API crudo**. La página ahora renderiza un snapshot inicial del servidor y luego refresca `/api/status`; si falla JavaScript, fetch o el endpoint, el error queda visible en **Último error / diagnóstico**.

## Deploy en Render

El archivo `render.yaml` incluye el servicio web y fija `PYTHON_VERSION=3.12.13` para evitar que Render use Python 3.14, donde dependencias con extensiones nativas pueden compilar desde fuente y fallar. En Render configura las variables de entorno necesarias y despliega el repositorio.

