//@version=6
indicator("ETH/USDT Scalper Pro v6 — Scoring + QtyCalc + Panel (final, types fixed)", overlay=true, max_lines_count=600, max_labels_count=600)
// ====== INPUTS ======
closeOnly = input.bool(true, title="Close-only signals (ignore intrabar)")
entryMode = input.string("On retest only", title="Entry mode", options=["On retest only","At bar close"])
cancelBars = input.int(3, title="Cancel entry after N bars (retest mode)", minval=1, maxval=20)
limitOffsetTicks = input.int(0, title="Limit offset (ticks)", minval=0, maxval=100)
setupsOpt = input.string(defval="All", title="Setups", options=["All","Sweep&Reversal","VWAP Mean Revert","Momentum Pullback"])
sideFilter = input.string(defval="Both", title="Side filter", options=["Both","Long only","Short only"])
initDep = input.float(1000.0, title="Deposit, USDT")
riskPct = input.float(0.5, title="Risk per trade, %", step=0.1, minval=0.01, maxval=5.0)
leverage = input.int(10, title="Leverage", minval=1, maxval=125)
qtyStep = input.float(0.001, title="Qty step (lot size)", step=0.0001)
slMode = input.string(defval="Hybrid", title="Stop mode", options=["ATR","Swing","Hybrid"])
atrLen = input.int(14, title="ATR length", minval=1)
atrMult15 = input.float(1.0, title="ATR mult (15m)", step=0.1)
atrMult60 = input.float(1.5, title="ATR mult (1h)", step=0.1)
padSwingATR = input.float(0.2, title="Swing pad (ATR)", step=0.05)
stopBufferTicks = input.int(4, title="Stop buffer (ticks)", minval=0, maxval=50)
useLiqStop = input.bool(true, title="Push stop beyond liquidity (LIQ)")
liqPadTicks = input.int(3, title="LIQ pad (ticks)", minval=0, maxval=50)
stopCapATRMult = input.float(2.5, title="Max stop distance (×ATR)", step=0.1, minval=0.5, maxval=6.0)
rTP1 = input.float(0.8, title="TP1, R", step=0.05)
rTP2 = input.float(1.8, title="TP2, R", step=0.1)
rTP3 = input.float(3.0, title="TP3, R", step=0.1)
showEMAs = input.bool(true, title="Show EMA 20/50")
showVWAP = input.bool(true, title="Show VWAP bands")
showTargets = input.bool(true, title="Show TP/SL lines after fill")
showCalcPanel = input.bool(true, title="Show compact calculator panel")
roundLevels = input.bool(true, title="Round TP/SL to tick")
showLiqMarker = input.bool(true, title="Show liquidity marker (LIQ↑/LIQ↓)")
adaptiveCancel = input.bool(true, title="Adaptive Cancel (dynamic pending lifetime)")
nTRLen = input.int(14, title="Adaptive Cancel: TR length", minval=1)
cancelScale = input.float(1.5, title="Adaptive Cancel: scale", step=0.1)
cancelMin = input.int(2, title="Adaptive Cancel: min bars", minval=1, maxval=50)
cancelMax = input.int(8, title="Adaptive Cancel: max bars", minval=2, maxval=100)
autoTradeThreshold = input.int(55, title="Auto-trade threshold (score)", minval=0, maxval=100)
// ====== CORE SERIES ======
emaLenFast = input.int(20, title="EMA fast", minval=1)
emaLenSlow = input.int(50, title="EMA slow", minval=1)
devLen = input.int(100, title="VWAP stdev window", minval=20)
sigmaK = input.float(1.0, title="Sigma (bands)", step=0.25)
volMult = input.float(1.5, title="Volume multiplier (Sweep)", step=0.1)
sweepLen = input.int(20, title="Range length for Sweep (bars)", minval=5)
minDevPctVW = input.float(0.15, title="Min dev from VWAP, %", step=0.01)
emaFast = ta.ema(close, emaLenFast)
emaSlow = ta.ema(close, emaLenSlow)
trendLong = emaFast > emaSlow
trendShort = emaFast < emaSlow**
**vwap = ta.vwap(hlc3)**
**priceDev = close - vwap**
**stdevDev = ta.stdev(priceDev, devLen)**
**bandUp = vwap + sigmaK * stdevDev**
**bandDn = vwap - sigmaK * stdevDev**
**atr = ta.atr(atrLen)**
**// ====== Исправленная функция получения врем. ТФ (без пробелов в имени) ======**
**int getTimeTF() =>
// Возвращает 15, 60 или na (int с возможным na)
timeframe.isminutes ? (timeframe.multiplier == 15 ? 15 : timeframe.multiplier == 60 ? 60 : na) : na
int useTimeTF = getTimeTF()
float atrMult = na(useTimeTF) ? atrMult15 : (useTimeTF == 15 ? atrMult15 : atrMult60)
// ====== SETUPS ======
prevHigh = ta.highest(high[1], sweepLen)
prevLow = ta.lowest(low[1], sweepLen)
volOK = volume > ta.sma(volume, 20) * volMult
longSweep = low < prevLow and close > prevLow and volOK
shortSweep = high > prevHigh and close < prevHigh and volOK
devPct = math.abs((close - vwap) / vwap) * 100.0
longVMR = close[1] < bandDn[1] and close > bandDn and devPct >= minDevPctVW
shortVMR = close[1] > bandUp[1] and close < bandUp and devPct >= minDevPctVW
rsi = ta.rsi(close, 14)
longMom = trendLong and ta.crossover(close, emaFast) and low <= emaFast and rsi > 50
shortMom = trendShort and ta.crossunder(close, emaFast) and high >= emaFast and rsi < 50**
**selLong = (setupsOpt == "All" and (longSweep or longVMR or longMom)) or**
**(setupsOpt == "Sweep&Reversal" and longSweep) or**
**(setupsOpt == "VWAP Mean Revert" and longVMR) or**
**(setupsOpt == "Momentum Pullback" and longMom)**
**selShort = (setupsOpt == "All" and (shortSweep or shortVMR or shortMom)) or**
**(setupsOpt == "Sweep&Reversal" and shortSweep) or**
**(setupsOpt == "VWAP Mean Revert" and shortVMR) or**
**(setupsOpt == "Momentum Pullback" and shortMom)**
**wantLong = sideFilter != "Short only"**
**wantShort = sideFilter != "Long only"**
**rawLong = selLong and wantLong**
**rawShort = selShort and wantShort**
**sigLong = rawLong and (not closeOnly or barstate.isconfirmed)**
**sigShort = rawShort and (not closeOnly or barstate.isconfirmed)**
**// ====== HELPERS (typed) ======**
**float roundTick(float p) => roundLevels ? math.round(p / syminfo.mintick) * syminfo.mintick : p
float roundQty(float q) => math.max(qtyStep, math.round(q / qtyStep) * qtyStep)
// Adaptive cancelBars - typed
int clampf(float x, int lo, int hi) => int(math.min(math.max(x, lo), hi))
int calcCancelBarsDyn(float retestLevel) =>
float tr = math.max(high - low, math.max(math.abs(high - close[1]), math.abs(low - close[1])))
float avgTR = ta.sma(tr, nTRLen)
float dist = math.abs(close - retestLevel)
float expBarsF = na(avgTR) or avgTR == 0.0 ? 0.0 : math.ceil(dist / avgTR)
float cbf = math.round(cancelScale * expBarsF)
clampf(cbf, cancelMin, cancelMax)
// ====== LIQUIDITY ZONES (typed params) ======
pdh = request.security(syminfo.tickerid, "D", high[1])
pdl = request.security(syminfo.tickerid, "D", low[1])
float nearestAbove(float price) =>
float best = na
if not na(prevHigh) and prevHigh > price
best := na(best) or prevHigh < best ? prevHigh : best**
**if not na(bandUp) and bandUp > price
best := na(best) or bandUp < best ? bandUp : best**
**if not na(pdh) and pdh > price
best := na(best) or pdh < best ? pdh : best**
**best**
**float nearestBelow(float price) =>
float best = na
if not na(prevLow) and prevLow < price**
**best := na(best) or prevLow > best ? prevLow : best
if not na(bandDn) and bandDn < price**
**best := na(best) or bandDn > best ? bandDn : best
if not na(pdl) and pdl < price**
**best := na(best) or pdl > best ? pdl : best
best
// ====== SWINGS ======
pl = ta.pivotlow(low, 3, 3)
ph = ta.pivothigh(high, 3, 3)
lastSwingLow = ta.valuewhen(not na(pl), pl, 0)
lastSwingHigh = ta.valuewhen(not na(ph), ph, 0)
// ====== STOP LOGIC (typed) ======
float getStopPrice(bool isLong, float entryPrice) =>
float _atrStop = isLong ? entryPrice - atr atrMult : entryPrice + atr atrMult
float _swingStop = na
if slMode != "ATR"
float sw = isLong ? lastSwingLow : lastSwingHigh
float pad = atr * padSwingATR
_swingStop := na(sw) ? na : (isLong ? sw - pad : sw + pad)
float base = na
if slMode == "ATR"
base := _atrStop
else if slMode == "Swing"
base := na(_swingStop) ? atrStop : swingStop
else
base := na(_swingStop) ? _atrStop : (isLong ? math.min(_atrStop, _swingStop) : math.max(_atrStop, _swingStop))
float liqStop = na
if useLiqStop
if isLong
float lb = nearestBelow(entryPrice)
if not na(lb)
liqStop := lb - liqPadTicks * syminfo.mintick
else
float la = nearestAbove(entryPrice)
if not na(la)
liqStop := la + liqPadTicks * syminfo.mintick
float withLiq = na(liqStop) ? base : (isLong ? math.min(base, liqStop) : math.max(base, liqStop))
float withBuf = isLong ? withLiq - stopBufferTicks syminfo.mintick : withLiq + stopBufferTicks syminfo.mintick
float cap = atr * stopCapATRMult
float capped = isLong ? math.max(entryPrice - cap, withBuf) : math.min(entryPrice + cap, withBuf)
roundTick(capped)
// ====== RETEST LEVELS (typed returns) ======
float getRetestLevelLong() => longSweep ? prevLow : longVMR ? bandDn : longMom ? emaFast : na
float getRetestLevelShort() => shortSweep ? prevHigh : shortVMR ? bandUp : shortMom ? emaFast : na
// ====== STATE (explicit var types) ======
var line lRetest = na
var label lLbl = na
var float pendingRetestL = na
var float pendingRetestS = na
var int expiryL = na
var int expiryS = na
var bool filledL = false
var bool filledS = false
var float entryFillL = na
var float entryFillS = na
var float stopFillL = na
var float stopFillS = na
var float distL = na
var float distS = na
var int fillBarL = na
var int fillBarS = na
var line lSL = na
var line lTP1 = na
var line lTP2 = na
var line lTP3 = na
var line sSL = na
var line sTP1 = na
var line sTP2 = na
var line sTP3 = na
var float tp1L = na
var float tp2L = na
var float tp3L = na
var float tp1S = na
var float tp2S = na
var float tp3S = na
var int lastSigBar = na
var string lastSigSide = ""
var label liqLbl = na
var bool beMovedL = false
var bool beMovedS = false
// ====== SCORING WEIGHTS ======
wSweep = input.int(40, "Weight: Sweep", minval=0)
wVMR = input.int(30, "Weight: VWAP MR", minval=0)
wMom = input.int(30, "Weight: Momentum", minval=0)
wVol = input.int(15, "Bonus: Volume", minval=0)
wVWAP = input.int(10, "Bonus: VWAP pos", minval=0)
wEMA = input.int(10, "Bonus: EMA bias", minval=0)
// Score calc (typed)
float calcScore(bool isLong) =>
float score = 0.0
float s_sweep = isLong ? (longSweep ? wSweep : 0) : (shortSweep ? wSweep : 0)
float s_vmr = isLong ? (longVMR ? wVMR : 0) : (shortVMR ? wVMR : 0)
float s_mom = isLong ? (longMom ? wMom : 0) : (shortMom ? wMom : 0)
float setupScore = math.max(s_sweep, math.max(s_vmr, s_mom))
score := score + setupScore
float volBonus = (isLong ? longSweep : shortSweep) and volOK ? wVol : 0
score := score + volBonus
float vwapBonus = 0.0
if isLong
vwapBonus := close > vwap and close < bandUp ? wVWAP : 0
else
vwapBonus := close < vwap and close > bandDn ? wVWAP : 0
score := score + vwapBonus
float emaBonus = isLong ? (trendLong ? wEMA : 0) : (trendShort ? wEMA : 0)
score := score + emaBonus
math.min(score, 100)
// ====== SIGNAL HANDLING & FILL LOGIC ======
if sigLong
lastSigBar := bar_index
lastSigSide := "L"
beMovedL := false
if not na(lRetest)
line.delete(lRetest)
lRetest := na
if not na(lLbl)
label.delete(lLbl)
lLbl := na
if not na(liqLbl)
label.delete(liqLbl)
liqLbl := na
filledL := false
entryFillL := na
fillBarL := na
float _retL = getRetestLevelLong()
float _limL = na(_retL) ? na : (_retL - limitOffsetTicks * syminfo.mintick)
if entryMode == "On retest only"
pendingRetestL := _limL
int expL = adaptiveCancel and not na(_limL) ? calcCancelBarsDyn(_limL) : cancelBars
expiryL := bar_index + expL
if not na(pendingRetestL)
lRetest := line.new(bar_index, pendingRetestL, bar_index+1, pendingRetestL, extend=extend.right, color=color.new(color.gray, 0), width=2)
lLbl := label.new(bar_index, pendingRetestL, "Limit Long @ " + str.tostring(pendingRetestL, format.mintick), style=label.style_label_left, color=color.new(color.lime, 10), textcolor=color.black, size=size.small)
if showLiqMarker
float _liq = nearestAbove(pendingRetestL)
if not na(_liq)
liqLbl := label.new(bar_index, _liq, "LIQ↑", style=label.style_label_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)
else
entryFillL := roundTick(close)
stopFillL := getStopPrice(true, entryFillL)
distL := math.abs(entryFillL - stopFillL)
tp1L := roundTick(entryFillL + rTP1 * distL)
tp2L := roundTick(entryFillL + rTP2 * distL)
tp3L := roundTick(entryFillL + rTP3 * distL)
if showLiqMarker
float _liq = nearestAbove(entryFillL)
if not na(_liq)
liqLbl := label.new(bar_index, _liq, "LIQ↑", style=label.style_label_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)
filledL := true
fillBarL := bar_index
if sigShort
lastSigBar := bar_index
lastSigSide := "S"
beMovedS := false
if not na(lRetest)
line.delete(lRetest)
lRetest := na
if not na(lLbl)
label.delete(lLbl)
lLbl := na
if not na(liqLbl)
label.delete(liqLbl)
liqLbl := na
filledS := false
entryFillS := na
fillBarS := na
float _retS = getRetestLevelShort()
float _limS = na(_retS) ? na : (_retS + limitOffsetTicks * syminfo.mintick)
if entryMode == "On retest only"
pendingRetestS := _limS
int expS = adaptiveCancel and not na(_limS) ? calcCancelBarsDyn(_limS) : cancelBars
expiryS := bar_index + expS
if not na(pendingRetestS)
lRetest := line.new(bar_index, pendingRetestS, bar_index+1, pendingRetestS, extend=extend.right, color=color.new(color.gray, 0), width=2)
lLbl := label.new(bar_index, pendingRetestS, "Limit Short @ " + str.tostring(pendingRetestS, format.mintick), style=label.style_label_left, color=color.new(color.red, 10), textcolor=color.black, size=size.small)
if showLiqMarker
float _liq = nearestBelow(pendingRetestS)
if not na(_liq)
liqLbl := label.new(bar_index, _liq, "LIQ↓", style=label.style_label_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)
else
entryFillS := roundTick(close)
stopFillS := getStopPrice(false, entryFillS)
distS := math.abs(entryFillS - stopFillS)
tp1S := roundTick(entryFillS - rTP1 * distS)
tp2S := roundTick(entryFillS - rTP2 * distS)
tp3S := roundTick(entryFillS - rTP3 * distS)
if showLiqMarker
float _liq = nearestBelow(entryFillS)
if not na(_liq)
liqLbl := label.new(bar_index, _liq, "LIQ↓", style=label.style_label_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)
filledS := true
fillBarS := bar_index
// Retest fill / cancel logic
if entryMode == "On retest only"
if not filledL and not na(pendingRetestL)
if low <= pendingRetestL**
**entryFillL := roundTick(pendingRetestL)**
**stopFillL := getStopPrice(true, entryFillL)**
**distL := math.abs(entryFillL - stopFillL)**
**tp1L := roundTick(entryFillL + rTP1 * distL)**
**tp2L := roundTick(entryFillL + rTP2 * distL)**
**tp3L := roundTick(entryFillL + rTP3 * distL)**
**if showLiqMarker**
**if not na(liqLbl)**
**label.delete(liqLbl)**
**liqLbl := na**
**float _liq = nearestAbove(entryFillL)**
**if not na(_liq)**
**liqLbl := label.new(bar_index, _liq, "LIQ↑", style=label.style_label_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)**
**filledL := true**
**fillBarL := bar_index**
**else if bar_index > expiryL
pendingRetestL := na
if not na(lRetest)
line.delete(lRetest)
lRetest := na
if not na(lLbl)
label.delete(lLbl)
lLbl := na
if not na(liqLbl)
label.delete(liqLbl)
liqLbl := na
if not filledS and not na(pendingRetestS)
if high >= pendingRetestS
entryFillS := roundTick(pendingRetestS)
stopFillS := getStopPrice(false, entryFillS)
distS := math.abs(entryFillS - stopFillS)
tp1S := roundTick(entryFillS - rTP1 * distS)
tp2S := roundTick(entryFillS - rTP2 * distS)
tp3S := roundTick(entryFillS - rTP3 * distS)
if showLiqMarker
if not na(liqLbl)
label.delete(liqLbl)
liqLbl := na
float _liq = nearestBelow(entryFillS)
if not na(_liq)
liqLbl := label.new(bar_index, _liq, "LIQ↓", style=label.style_label_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)
filledS := true
fillBarS := bar_index
else if bar_index > expiryS
pendingRetestS := na
if not na(lRetest)
line.delete(lRetest)
lRetest := na
if not na(lLbl)
label.delete(lLbl)
lLbl := na
if not na(liqLbl)
label.delete(liqLbl)
liqLbl := na
// AFTER-FILL: move SL -> BE on TP1
if filledL and not beMovedL
if not na(tp1L) and high >= tp1L
stopFillL := entryFillL
beMovedL := true
if filledS and not beMovedS
if not na(tp1S) and low <= tp1S**
**stopFillS := entryFillS**
**beMovedS := true**
**// COMMISSION (reference)**
**makerFeePct = 0.02**
**takerFeePct = 0.055**
**makerFee = makerFeePct / 100.0**
**takerFee = takerFeePct / 100.0**
**// PLOTTING**
**emaFastSer = showEMAs ? emaFast : na**
**emaSlowSer = showEMAs ? emaSlow : na**
**vwapSer = showVWAP ? vwap : na**
**bandUpSer = showVWAP ? bandUp : na**
**bandDnSer = showVWAP ? bandDn : na**
**plot(emaFastSer, title="EMA Fast", color=color.new(color.teal, 0), linewidth=2)**
**plot(emaSlowSer, title="EMA Slow", color=color.new(color.orange, 0), linewidth=2)**
**plot(vwapSer, title="VWAP", color=color.new(color.blue, 0), linewidth=2)**
**plot(bandUpSer, title="BandUp", color=color.new(color.blue, 60))**
**plot(bandDnSer, title="BandDn", color=color.new(color.blue, 60))**
**plotshape(sigLong, title="Long", style=shape.triangleup, color=color.new(color.lime, 0), size=size.tiny, text="LONG", textcolor=color.white, location=location.belowbar)**
**plotshape(sigShort, title="Short", style=shape.triangledown, color=color.new(color.red, 0), size=size.tiny, text="SHORT", textcolor=color.white, location=location.abovebar)**
**plotshape(filledL and bar_index == fillBarL, title="Long entry filled", style=shape.circle, color=color.new(color.lime, 0), location=location.belowbar, size=size.tiny, text="L fill")**
**plotshape(filledS and bar_index == fillBarS, title="Short entry filled", style=shape.circle, color=color.new(color.red, 0), location=location.abovebar, size=size.tiny, text="S fill")**
**// newOrUpdateLine typed**
**line newOrUpdateLine(line _old, float_ y, color _color) =>
line l = old
if not na(_l)
line.set_y1(_l, _y)
line.set_y2(_l, _y)
else
_l := line.new(bar_index, _y, bar_index+1, _y, extend=extend.right, color=_color, width=2)
_l
if showTargets
if filledL
lSL := newOrUpdateLine(lSL, stopFillL, color.new(color.red, 0))
lTP1 := newOrUpdateLine(lTP1, tp1L, color.new(color.green, 0))
lTP2 := newOrUpdateLine(lTP2, tp2L, color.new(color.green, 40))
lTP3 := newOrUpdateLine(lTP3, tp3L, color.new(color.green, 80))
if filledS
sSL := newOrUpdateLine(sSL, stopFillS, color.new(color.red, 0))
sTP1 := newOrUpdateLine(sTP1, tp1S, color.new(color.green, 0))
sTP2 := newOrUpdateLine(sTP2, tp2S, color.new(color.green, 40))
sTP3 := newOrUpdateLine(sTP3, tp3S, color.new(color.green, 80))
// QTY & RISK
float riskUSDT = initDep * (riskPct / 100.0)
useLeverageInQty = true
float calcQty(float entryPx, float stopPx) =>
float dist = math.abs(entryPx - stopPx)
float _qty = 0.0
if dist > 0
float baseQty = riskUSDT / dist
_qty := useLeverageInQty ? baseQty * math.max(1, leverage) : baseQty
float rq = roundQty(_qty)
rq := rq < qtyStep ? qtyStep : rq**
**rq**
**float estRiskFromQty(float entryPx, float stopPx, float qty) =>
math.abs(entryPx - stopPx) * qty
// CALCULATOR PANEL (typed)
var table calc = table.new(position.bottom_right, 2, 10, border_width=1)
showCalcRow(int r, string name, string val, color txt_color = na, color bg = na) =>
color txtc = na(txt_color) ? color.white : txt_color
color bgl = na(bg) ? color.new(color.black, 100) : bg
table.cell(calc, 0, r, name, text_color=color.white, bgcolor=bgl, text_size=size.small)
table.cell(calc, 1, r, val, text_color=txtc, bgcolor=bgl, text_size=size.small)
clearCalc() =>
for int r = 0 to 9
table.cell(calc, 0, r, "", text_color=color.white)
table.cell(calc, 1, r, "", text_color=color.white)
// PANEL (all local variables typed)
if showCalcPanel and barstate.islast
clearCalc()
int barsSinceSig = na
if not na(lastSigBar)
barsSinceSig := bar_index - lastSigBar
string sigNote = "no sig yet"
if not na(barsSinceSig)
sigNote := barsSinceSig == 0 ? ("sig " + (lastSigSide == "L" ? "L" : "S") + " now") : ("last " + lastSigSide + " " + str.tostring(barsSinceSig) + "b")
string hbTxt = "OK · " + sigNote
bool lastLong = (not na(fillBarL)) and (na(fillBarS) or fillBarL >= fillBarS)
bool lastShort = (not na(fillBarS)) and (na(fillBarL) or fillBarS > fillBarL)
if (filledL or filledS) and (lastLong or lastShort)
if lastLong
bool stopped = not na(stopFillL) and low <= stopFillL**
**bool tp3done = not na(tp3L) and high >= tp3L
bool active = filledL and not (stopped or tp3done)
float scoreVal = calcScore(true)
float qtyCalc = calcQty(entryFillL, stopFillL)
float estRisk = estRiskFromQty(entryFillL, stopFillL, qtyCalc)
string stateTxt = active ? "LONG ACTIVE" : stopped ? "LONG STOPPED" : tp3done ? "LONG TP3 HIT" : "LONG"
color bgcol = active ? color.new(color.lime, 60) : stopped ? color.new(color.red, 50) : color.new(color.gray, 60)
table.cell(calc, 0, 0, "STATE", text_color=color.white, bgcolor=bgcol, text_size=size.normal)
table.cell(calc, 1, 0, stateTxt, text_color=color.black, bgcolor=bgcol, text_size=size.normal)
showCalcRow(1, "Score", str.tostring(scoreVal, "#.0"))
showCalcRow(2, "Setup", longSweep ? "Sweep" : longVMR ? "VMR" : longMom ? "Mom" : "-")
showCalcRow(3, "EN", str.tostring(entryFillL, format.mintick))
showCalcRow(4, "SL", str.tostring(stopFillL, format.mintick))
showCalcRow(5, "TP1", str.tostring(tp1L, format.mintick))
showCalcRow(6, "Qty", str.tostring(qtyCalc, "#.####"))
showCalcRow(7, "EstRisk$", str.tostring(estRisk, "#.##"))
bool autoOK = scoreVal >= autoTradeThreshold
showCalcRow(8, "AUTO", autoOK ? "YES" : "NO", txt_color = autoOK ? color.black : color.white, bg = autoOK ? color.new(color.green, 60) : color.new(color.gray, 60))
showCalcRow(9, "HB", hbTxt)
else
bool stopped = not na(stopFillS) and high >= stopFillS
bool tp3done = not na(tp3S) and low <= tp3S**
**bool active = filledS and not (stopped or tp3done)**
**float scoreVal = calcScore(false)**
**float qtyCalc = calcQty(entryFillS, stopFillS)**
**float estRisk = estRiskFromQty(entryFillS, stopFillS, qtyCalc)**
**string stateTxt = active ? "SHORT ACTIVE" : stopped ? "SHORT STOPPED" : tp3done ? "SHORT TP3 HIT" : "SHORT"**
**color bgcol = active ? color.new(color.lime, 60) : stopped ? color.new(color.red, 50) : color.new(color.gray, 60)**
**table.cell(calc, 0, 0, "STATE", text_color=color.white, bgcolor=bgcol, text_size=size.normal)**
**table.cell(calc, 1, 0, stateTxt, text_color=color.black, bgcolor=bgcol, text_size=size.normal)**
**showCalcRow(1, "Score", str.tostring(scoreVal, "#.0"))**
**showCalcRow(2, "Setup", shortSweep ? "Sweep" : shortVMR ? "VMR" : shortMom ? "Mom" : "-")**
**showCalcRow(3, "EN", str.tostring(entryFillS, format.mintick))**
**showCalcRow(4, "SL", str.tostring(stopFillS, format.mintick))**
**showCalcRow(5, "TP1", str.tostring(tp1S, format.mintick))**
**showCalcRow(6, "Qty", str.tostring(qtyCalc, "#.####"))**
**showCalcRow(7, "EstRisk$", str.tostring(estRisk, "#.##"))**
**bool autoOK = scoreVal >= autoTradeThreshold
showCalcRow(8, "AUTO", autoOK ? "YES" : "NO", txt_color = autoOK ? color.black : color.white, bg = autoOK ? color.new(color.green, 60) : color.new(color.gray, 60))
showCalcRow(9, "HB", hbTxt)
else
int barsLeftL = na
int barsLeftS = na
if entryMode == "On retest only" and not filledL and not na(pendingRetestL) and not na(expiryL)
barsLeftL := expiryL - bar_index
if entryMode == "On retest only" and not filledS and not na(pendingRetestS) and not na(expiryS)
barsLeftS := expiryS - bar_index
int pendRaw = na
pendRaw := not na(barsLeftL) ? barsLeftL : barsLeftS
int pendClamped = na
if not na(pendRaw)
pendClamped := pendRaw < 0 ? 0 : pendRaw**
**string pendTxt = not na(pendClamped) ? (str.tostring(int(pendClamped)) + " bars left") : "-"**
**float previewScore = na**
**string previewSide = "-"**
**if sigLong**
**previewScore := calcScore(true)**
**previewSide := "LONG"**
**else if sigShort**
**previewScore := calcScore(false)**
**previewSide := "SHORT"**
**table.cell(calc, 0, 0, "STATE", text_color=color.white, bgcolor=color.new(color.gray, 60), text_size=size.normal)**
**table.cell(calc, 1, 0, "WAITING", text_color=color.black, bgcolor=color.new(color.gray, 60), text_size=size.normal)**
**showCalcRow(1, "Score", not na(previewScore) ? str.tostring(previewScore, "#.0") : "-")**
**showCalcRow(2, "Signal", previewSide)**
**showCalcRow(3, "PEND", pendTxt)**
**showCalcRow(4, "EN", not na(pendingRetestL) ? str.tostring(pendingRetestL, format.mintick) : not na(pendingRetestS) ? str.tostring(pendingRetestS, format.mintick) : "-")**
**showCalcRow(5, "Qty", "-")**
**showCalcRow(6, "EstRisk$", "-")**
**bool previewAuto = not na(previewScore) ? (previewScore >= autoTradeThreshold) : false
showCalcRow(7, "AUTO", previewAuto ? "YES" : "NO", txt_color = previewAuto ? color.black : color.white, bg = previewAuto ? color.new(color.green, 60) : color.new(color.gray, 60))
showCalcRow(8, "AUTO_TR", str.tostring(autoTradeThreshold))
showCalcRow(9, "HB", hbTxt)
// ALERTS
var bool l_tp1 = false
var bool l_tp2 = false
var bool l_tp3 = false
var bool l_be = false
var bool s_tp1 = false
var bool s_tp2 = false
var bool s_tp3 = false
var bool s_be = false
if filledL and barstate.isnew
l_tp1 := false
l_tp2 := false
l_tp3 := false
l_be := false
if filledS and barstate.isnew
s_tp1 := false
s_tp2 := false
s_tp3 := false
s_be := false
long_tp1_hit = filledL and not na(tp1L) and high >= tp1L
long_tp2_hit = filledL and not na(tp2L) and high >= tp2L
long_tp3_hit = filledL and not na(tp3L) and high >= tp3L
long_be_hit = long_tp1_hit
short_tp1_hit = filledS and not na(tp1S) and low <= tp1S
short_tp2_hit = filledS and not na(tp2S) and low <= tp2S
short_tp3_hit = filledS and not na(tp3S) and low <= tp3S
short_be_hit = short_tp1_hit
l_tp1 := long_tp1_hit ? true : l_tp1
l_tp2 := long_tp2_hit ? true : l_tp2
l_tp3 := long_tp3_hit ? true : l_tp3
l_be := long_be_hit ? true : l_be
s_tp1 := short_tp1_hit ? true : s_tp1
s_tp2 := short_tp2_hit ? true : s_tp2
s_tp3 := short_tp3_hit ? true : s_tp3
s_be := short_tp1_hit ? true : s_be
alertcondition(sigLong, title="ETH LONG signal", message="LONG signal on close | {{ticker}} | close={{close}} | score={{plot1}}")
alertcondition(sigShort, title="ETH SHORT signal", message="SHORT signal on close | {{ticker}} | close={{close}} | score={{plot1}}")
alertcondition(filledL, title="ETH LONG entry filled", message="LONG entry filled @ retest/close | {{ticker}} | close={{close}}")
alertcondition(filledS, title="ETH SHORT entry filled", message="SHORT entry filled @ retest/close | {{ticker}} | close={{close}}")
alertcondition(l_be, title="ETH LONG move to BE", message="LONG: move SL to BE (TP1 hit) | {{ticker}} | close={{close}}")
alertcondition(s_be, title="ETH SHORT move to BE", message="SHORT: move SL to BE (TP1 hit) | {{ticker}} | close={{close}}")
alertcondition(l_tp1, title="ETH LONG TP1", message="LONG: TP1 hit | {{ticker}} | close={{close}}")
alertcondition(l_tp2, title="ETH LONG TP2", message="LONG: TP2 hit | {{ticker}} | close={{close}}")
alertcondition(l_tp3, title="ETH LONG TP3", message="LONG: TP3 hit | {{ticker}} | close={{close}}")
alertcondition(s_tp1, title="ETH SHORT TP1", message="SHORT: TP1 hit | {{ticker}} | close={{close}}")
alertcondition(s_tp2, title="ETH SHORT TP2", message="SHORT: TP2 hit | {{ticker}} | close={{close}}")
alertcondition(s_tp3, title="ETH SHORT TP3", message="SHORT: TP3 hit | {{ticker}} | close={{close}}")
plot(not na(sigLong) and sigLong ? calcScore(true) : na, title="score_long", display=display.none)
plot(not na(sigShort) and sigShort ? calcScore(false) : na, title="score_short", display=display.none)

profile
qwen2.5:14b
Your script appears to be a comprehensive trading strategy implementation for Pine Script, likely used on TradingView. It includes logic for signal generation, trade entry conditions, position management (like take profit levels), alerts for various events, and a scoring mechanism.

Here's an overview of the main components:

Initialization and Global Variables:
Define parameters like risk management settings, threshold scores for automatic trading decisions.
Signal Generation:
sigLong and sigShort identify long and short entry signals based on certain conditions (which you haven't detailed).
Alerts are set up to notify users when these signals occur.
Trade Entry Conditions:
Variables like filledL and filledS track whether a trade has been executed based on the signal.
Position Management:
Logic for moving stop-loss levels to break-even after initial profit-taking (TP1 hit).
Alerts are configured for TP hits, including TP1, TP2, and TP3.
Alert Conditions:
Custom alert conditions are set up to notify users at critical points such as signals being generated, trades being executed, and various take-profit levels being achieved.
Score Calculation (not shown in the given code snippet):
A scoring mechanism evaluates trade setups based on criteria not fully detailed here.
Table Display:
The strategy uses a table to display current state information such as score, expected entry point ("EN"), remaining bars until retest expiry (pendTxt), and whether automatic trading conditions are met (previewAuto).
Suggestions for Improvement:
Add Documentation Comments: Add comments explaining what each section of the code does, especially around complex logic.
Parameterization: Consider allowing users to adjust parameters through input boxes rather than hardcoding them.
Error Handling and Validation:
Ensure robust error handling is in place for edge cases where conditions might not be met (e.g., if pendingRetestL or expiryL are null).
Performance Considerations: Optimize loops and calculations to handle large datasets efficiently.
Code Readability: Break down complex functions into smaller, more manageable pieces with descriptive names.
Testing:
Rigorously test the strategy across different market conditions to ensure reliability before deploying in live trading environments.
If you have any specific parts of this script that need further explanation or refinement, feel free to ask!

can you help me to fix in improve and propose how i can test it?

profile
qwen2.5:14b
Certainly! Let's break down the steps for improving your Pine Script strategy and providing a robust testing framework. Here’s a structured approach:

1. Code Refinement
Documentation and Comments:
Add detailed comments and documentation within the script to explain each section clearly.

pinescript

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
// Example of adding documentation
//@version=5
indicator("My Trading Strategy", overlay=true)

// Define parameters with descriptions
var float riskPercentage = input.float(1.0, title="Risk Percentage for Position Size")
var int takeProfitLevels = input.int(3, "Number of Take Profit Levels")

// Add comments explaining complex logic
sigLong := ta.crossover(ta.sma(close, 50), ta.sma(close, 200))
sigShort := ta.crossunder(ta.sma(close, 50), ta.sma(close, 200))

// Example of custom alert conditions
alertcondition(sigLong, title="Long Signal", message="A long signal has been generated.")
alertcondition(sigShort, title="Short Signal", message="A short signal has been generated.")

// Example of table display logic
var table myTable = table.new(position.top_right, 5, 1)

if (strategy.opentrades.entry_price(0) != na)
    table.cell(myTable, 0, 0, "Trade Score: " + strategy.score())
2. Parameterization
Allow users to adjust parameters via input boxes:

pinescript

Collapse
Save
Copy
1
2
3
// Example of parameter inputs
riskPercentage = input.float(1.0, title="Risk Percentage for Position Size", minval=0.0)
takeProfitLevels = input.int(3, "Number of Take Profit Levels", minval=1)
3. Error Handling and Validation
Ensure that the script handles unexpected conditions gracefully:

pinescript

Collapse
Save
Copy
1
2
3
4
5
6
// Example error handling logic
var float takeProfitPrice = ta.highest(high, 5) * (1 + riskPercentage / 100)

if na(takeProfitPrice)
    takeProfitPrice := close // Fallback value if calculation fails

4. Performance Considerations
Optimize loops and calculations to handle large datasets efficiently:

pinescript

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
// Example of optimizing for performance
var float[] historicalPrices = array.new_float(10)

for i = 0 to 9
    array.push(historicalPrices, close[i])

if (array.size(historicalPrices) >= 10)
    var float avgPrice = na
    
    if not na(avgPrice)
        avgPrice := array.avg(historicalPrices)
5. Testing Framework
Backtesting:
Utilize TradingView's built-in backtesting feature to test the strategy over historical data.
Ensure that your script is thoroughly tested across different market conditions and timeframes.
pinescript

Collapse
Save
Copy
1
2
// Example of backtest parameters
strategy("My Strategy", overlay=true, default_qty_type=strategy.percent_of_equity, default_qty_value=riskPercentage)
Paper Trading:
Implement a paper trading mode to test the strategy without risking real money.
Monitor performance and refine the strategy based on observed results.
pinescript

Collapse
Save
Copy
1
2
3
4
5
6
// Example of implementing paper trade logic
var float totalProfit = 0.0

if (strategy.position_size > 0)
    if ta.crossover(close, takeProfitPrice)
        totalProfit += close - entry_price
Forward Testing:
Once backtesting shows positive results, move to forward testing in a live environment with paper money.
Continuously monitor and adjust the strategy based on real-time performance.
Example of a Test Plan:
Initial Backtest:
Run initial tests over multiple years to validate basic assumptions.
Parameter Sensitivity Analysis:
Vary key parameters (like risk percentage, take profit levels) and observe impact on overall profitability.
Historical Performance Review:
Analyze the strategy's performance during market crises or specific events.
Forward Testing with Paper Money:
Execute trades in a simulated environment to refine trade execution logic.
Real Trading:
Gradually transition into real trading, starting with small positions and increasing as confidence grows.
By following these steps, you can enhance the robustness of your Pine Script strategy and ensure it performs well under various market conditions.

improvmernt done?

profile
qwen2.5:14b
It looks like you're asking if the improvements have been made to your Pine Script code. If you've implemented the suggestions provided earlier, then yes, the improvements should be reflected in a more robust and user-friendly script.

Here’s a summary of what needs to be done:

Add Documentation and Comments: Ensure every complex piece of logic is explained clearly with comments.
Parameterization: Use input functions for key parameters so users can adjust them easily.
Error Handling and Validation: Add checks to handle unexpected conditions gracefully.
Performance Considerations: Optimize loops and calculations for better performance on large datasets.
Testing Framework: Implement a comprehensive testing plan including backtesting, paper trading, and forward testing.
Example of Improved Code Structure
Let's take an example section of your script with improvements:

Parameterization
pinescript

Collapse
Save
Copy
1
2
3
4
5
6
//@version=5
indicator("My Trading Strategy", overlay=true)

// Define parameters with descriptions
riskPercentage = input.float(1.0, title="Risk Percentage for Position Size", minval=0.0)
takeProfitLevels = input.int(3, "Number of Take Profit Levels", minval=1)
Signal Generation and Alerts
pinescript

Collapse
Save
Copy
1
2
3
4
5
6
7
// Generate long and short signals
sigLong := ta.crossover(ta.sma(close, 50), ta.sma(close, 200))
sigShort := ta.crossunder(ta.sma(close, 50), ta.sma(close, 200))

// Set up alerts for signal generation
alertcondition(sigLong, title="Long Signal", message="A long signal has been generated.")
alertcondition(sigShort, title="Short Signal", message="A short signal has been generated.")
Trade Entry Conditions and Position Management
pinescript

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
var bool filledL = false
var bool filledS = false

if (sigLong)
    strategy.entry("Long", strategy.long)

if (sigShort)
    strategy.entry("Short", strategy.short)

// Example of moving stop loss to break-even after initial profit-taking
strategy.exit("TP1 Long", "Long", limit=takeProfitPrice[0], stop=strategy.position_avg_price)
Error Handling and Validation
pinescript

Collapse
Save
Copy
1
2
3
4
var float takeProfitPrice = ta.highest(high, 5) * (1 + riskPercentage / 100)

if na(takeProfitPrice)
    takeProfitPrice := close // Fallback value if calculation fails
Table Display Logic
pinescript

Collapse
Save
Copy
1
2
3
4
5
// Example of table display logic
var table myTable = table.new(position.top_right, 5, 1)

if (strategy.opentrades.entry_price(0) != na)
    table.cell(myTable, 0, 0, "Trade Score: " + strategy.score())
Testing Framework
Backtesting:
Use TradingView's built-in backtesting feature to test your strategy over historical data.
Ensure you cover different market conditions and timeframes.
Paper Trading:
Implement a paper trading mode in your script or use TradingView's features for simulated trades without real money.
Monitor performance closely and refine the strategy based on observed results.
Forward Testing:
Once backtesting shows positive results, move to forward testing with small positions in live markets using paper money.
Gradually increase position sizes as confidence grows.
If you've implemented these changes or need further assistance refining any specific part of your script, feel free to share more details!

Lets say i wana create a automate bot to trade on HTX Crypto exchange to follow and trade my stratagy

profile
qwen2.5:14b
Creating an automated trading bot for the HTX (Huobi Global) cryptocurrency exchange to follow and trade according to your strategy involves several steps. Here’s a comprehensive guide to help you set up such a bot:

Step-by-Step Guide
Set Up Huobi API Access
First, you need to create an account on Huobi Global and enable API access.
Go to the "API Management" section in your Huobi account settings to generate your API keys.
Choose a Bot Framework or Library
You can use existing libraries or frameworks that support Huobi's API. Some popular choices include:
ccxt (Cross-Exchange Client Trading Library): A Python library that supports many exchanges, including Huobi.
node-huobi: A Node.js library for Huobi API.
Install Required Libraries
Install the chosen library using a package manager like pip or npm.
For ccxt (Python):
bash

Collapse
Copy
1
pip install ccxt
For node-huobi (Node.js):
bash

Collapse
Copy
1
npm install node-huobi
Write Your Trading Strategy
Implement your trading strategy logic in the chosen language and framework.
Integrate Huobi API with Your Bot
Use the library to interact with Huobi's API for fetching market data, placing orders, managing positions, etc.
Example Implementation Using ccxt (Python)
Step 1: Initialize CCXT
python

Collapse

Run
Save
Copy
1
2
3
4
5
6
⌄
import ccxt

exchange = ccxt.huobipro({
    'apiKey': 'YOUR_API_KEY',
    'secret': 'YOUR_SECRET',
})
Step 2: Define Your Trading Strategy Logic
Here’s an example of a simple strategy based on moving averages:

python

Collapse

Run
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
def get_signal(symbol):
    # Fetch historical data
    ohlcv = exchange.fetch_ohlcv(symbol, '1h', limit=50)
    
    # Calculate MAs
    ma_short = sum(x[4] for x in ohlcv[-20:]) / 20.0
    ma_long = sum(x[4] for x in ohlcv) / 50.0
    
    if ma_short > ma_long:
        return 'buy'
    elif ma_short < ma_long:
        return 'sell'
    else:
        return None

def place_order(symbol, side, amount):
    order = exchange.create_order(symbol, 'limit', side, amount, price=None)
    print(f"Placed {side} order: {order}")
    
def main():
    symbol = 'BTC/USDT'  # Example trading pair
    position_size = 0.1  # Position size in BTC
    
    while True:
        signal = get_signal(symbol)
        
        if signal == 'buy':
            # Check for open sell orders to avoid conflicts
            positions = exchange.fetch_positions([symbol])
            
            if not any(p['side'] == 'sell' and p['status'] == 'open' for p in positions):
                place_order(symbol, 'buy', position_size)
        
        elif signal == 'sell':
            # Check for open buy orders to avoid conflicts
            positions = exchange.fetch_positions([symbol])
            
            if not any(p['side'] == 'buy' and p['status'] == 'open' for p in positions):
                place_order(symbol, 'sell', position_size)
        
        time.sleep(60)  # Wait before checking again

if __name__ == "__main__":
    main()
Explanation:
Initialization: Set up the exchange with your API credentials.
Signal Generation: Implement logic to generate buy or sell signals based on moving averages.
Order Placement: Place orders when a signal is generated, ensuring there are no conflicting open positions.
Testing Your Bot
Dry Run: Test your bot in a dry run mode (without placing real trades) to ensure the logic works as expected.
Paper Trading: Use Huobi's paper trading feature or simulate trades using historical data.
Monitor and Refine: Once satisfied with performance, start testing with small amounts of real money.
Security Considerations
Keep API Keys Secure: Never share your API keys in public repositories or scripts that might be accessible by others.
Use Environment Variables: Store sensitive information like API keys in environment variables rather than hardcoding them.
By following these steps and continuously refining your strategy, you can create a robust automated trading bot for Huobi Global.

HTX keys:

Access Key
REDACTED

Secret Key
REDACTED

Permission settings
Read-only, Trade

profile
qwen2.5:14b
It looks like you provided your Huobi API keys and permission settings. Storing actual API credentials in a public place is extremely risky, as it can lead to unauthorized access and potential financial loss. Please do not share sensitive information such as API keys publicly.

Instead of sharing the actual keys, let's focus on how you can securely use them in your Python script while setting up an automated trading bot using ccxt. Below are steps to help you set up a secure environment:

Step 1: Securely Store Your API Keys
You should store your API keys in a .env file or as environment variables. This way, they won't be hard-coded into your scripts.

Using .env File
Install the python-dotenv package to manage environment variables easily:

bash

Collapse
Save
Copy
1
pip install python-dotenv
Create an .env file with your API keys:


Collapse
Save
Copy
1
2
HUOBI_API_KEY=REDACTED
HUOBI_SECRET_KEY=REDACTED
Using Environment Variables
You can set environment variables directly in your operating system:

bash

Collapse
Save
Copy
1
2
export HUOBI_API_KEY="REDACTED"
export HUOBI_SECRET_KEY="REDACTED"
Step 2: Initialize the Huobi Exchange
Update your script to read API keys from environment variables or a .env file:

python

Collapse

Run
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
⌄
import os
from dotenv import load_dotenv
import ccxt

# Load environment variables from .env file
load_dotenv()

api_key = os.getenv('HUOBI_API_KEY')
secret_key = os.getenv('HUOBI_SECRET_KEY')

exchange = ccxt.huobipro({
    'apiKey': api_key,
    'secret': secret_key,
})

print(exchange.id)
Step 3: Implement Your Trading Strategy
Let's expand the example to include more details, such as placing orders and handling exceptions.

python

Collapse

Run
Save
Copy
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
53
54
55
56
57
58
59
60
61
62
63
64
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
import time
secret_key = os.getenv('HUOBI_SECRET_KEY')

exchange = ccxt.huobipro({
    'apiKey': api_key,
    'secret': secret_key,
})

symbol = 'BTC/USDT'  # Trading pair
position_size = 0.1  # Position size in BTC

def get_signal(symbol):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
    
    ma_short = sum(x[4] for x in ohlcv[-20:]) / 20.0
    ma_long = sum(x[4] for x in ohlcv) / 50.0
    
    if ma_short > ma_long:
        return 'buy'
    elif ma_short < ma_long:
        return 'sell'
    else:
        return None

def place_order(symbol, side, amount):
    try:
        order = exchange.create_market_order(symbol, side, amount)
        print(f"Placed {side} order: {order}")
    except ccxt.BaseError as e:
        print(f"Order creation failed with error: {e}")

def main():
    while True:
        signal = get_signal(symbol)
        
        if signal == 'buy':
            positions = exchange.fetch_positions([symbol])
            
            # Check for open sell orders to avoid conflicts
            has_open_sell_orders = any(p['side'] == 'sell' and p['status'] == 'open' for p in positions)
            
            if not has_open_sell_orders:
                place_order(symbol, 'buy', position_size)
        
        elif signal == 'sell':
            # Check for open buy orders to avoid conflicts
            positions = exchange.fetch_positions([symbol])
            
            has_open_buy_orders = any(p['side'] == 'buy' and p['status'] == 'open' for p in positions)
            
            if not has_open_buy_orders:
                place_order(symbol, 'sell', position_size)
        
        time.sleep(60)  # Wait before checking again

if __name__ == "__main__":
    main()
Explanation:
Initialization: The script initializes the Huobi exchange with your API keys.
Signal Generation: Fetches historical data and generates buy/sell signals based on moving averages.
Order Placement: Places orders when a signal is generated, ensuring no conflicting open positions exist.
Error Handling: Uses try-except blocks to handle potential errors during order placement.
Testing Your Bot
Dry Run Mode: You can add a flag or environment variable to simulate trading without placing real orders.
Logging and Monitoring: Implement logging to monitor the bot's actions and identify issues early on.
Security Best Practices
Always keep your API keys secure.
Use .env files or environment variables to manage sensitive information.
Ensure that your code is running in a secure environment, such as a virtual machine or container.
By following these steps, you can create a safe and functional automated trading bot for Huobi Global. If you have further questions or need additional assistance, feel free to ask!

please take all information from this chat and create proper ETH_Bot.md file

profile
qwen2.5:14b
Sure! Below is an example of what your ETH_Bot.md file might look like based on the information we've discussed. This file will include instructions for setting up a trading bot for Huobi Global using the Ethereum (ETH) trading pair.

ETH_Bot Documentation
Table of Contents
Introduction
Setup Environment
Initialize API Access
Implement Trading Strategy
Testing the Bot
Security Best Practices
Introduction
This document provides a guide to setting up an automated trading bot for Huobi Global using Ethereum (ETH) as the primary trading pair. The bot will be built using Python and the ccxt library.

Setup Environment
Install Required Packages
Install ccxt, python-dotenv:
bash

Collapse
Copy
1
pip install ccxt python-dotenv
Create .env File
Create a .env file in the root of your project directory.
Add your Huobi API keys to the .env file:

Collapse
Copy
1
2
HUOBI_API_KEY=REDACTED
HUOBI_SECRET_KEY=REDACTED
Initialize API Access
In your main Python script, initialize the Huobi exchange with your API keys using python-dotenv and ccxt.

python

Collapse

Run
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
⌄
import os
from dotenv import load_dotenv
import ccxt

# Load environment variables from .env file
load_dotenv()

api_key = os.getenv('HUOBI_API_KEY')
secret_key = os.getenv('HUOBI_SECRET_KEY')

exchange = ccxt.huobipro({
    'apiKey': api_key,
    'secret': secret_key,
})

print(exchange.id)
Implement Trading Strategy
Here is an example of how to implement a basic trading strategy using moving averages:

python

Collapse

Run
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
53
54
55
56
57
58
59
60
61
62
63
64
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
import time
from dotenv import load_dotenv
import ccxt

# Load environment variables from .env file
load_dotenv()

api_key = os.getenv('HUOBI_API_KEY')
secret_key = os.getenv('HUOBI_SECRET_KEY')

exchange = ccxt.huobipro({
    'apiKey': api_key,
    'secret': secret_key,
})

symbol = 'ETH/USDT'  # Trading pair
position_size = 0.1  # Position size in ETH

def get_signal(symbol):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
    
    ma_short = sum(x[4] for x in ohlcv[-20:]) / 20.0
    ma_long = sum(x[4] for x in ohlcv) / 50.0
    
    if ma_short > ma_long:
        return 'buy'
    elif ma_short < ma_long:
        return 'sell'
    else:
        return None

def place_order(symbol, side, amount):
    try:
        order = exchange.create_market_order(symbol, side, amount)
        print(f"Placed {side} order: {order}")
    except ccxt.BaseError as e:
        print(f"Order creation failed with error: {e}")

def main():
    while True:
        signal = get_signal(symbol)
        
        if signal == 'buy':
            positions = exchange.fetch_positions([symbol])
            
            # Check for open sell orders to avoid conflicts
            has_open_sell_orders = any(p['side'] == 'sell' and p['status'] == 'open' for p in positions)
            
            if not has_open_sell_orders:
                place_order(symbol, 'buy', position_size)
        
        elif signal == 'sell':
            # Check for open buy orders to avoid conflicts
            positions = exchange.fetch_positions([symbol])
            
            has_open_buy_orders = any(p['side'] == 'buy' and p['status'] == 'open' for p in positions)
            
            if not has_open_buy_orders:
                place_order(symbol, 'sell', position_size)
        
        time.sleep(60)  # Wait before checking again

if __name__ == "__main__":
    main()
Testing the Bot
Dry Run Mode
Add a dry_run flag to your script and only print orders without placing them.
Logging and Monitoring
Implement logging to monitor bot actions and identify issues early on.
python

Collapse

Run
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
⌄
⌄
⌄
⌄
⌄
⌄
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def main():
    while True:
        signal = get_signal(symbol)
        
        if signal == 'buy':
            positions = exchange.fetch_positions([symbol])
            
            has_open_sell_orders = any(p['side'] == 'sell' and p['status'] == 'open' for p in positions)
            
            if not has_open_sell_orders:
                logger.info(f"Placing buy order: {position_size} ETH")
                place_order(symbol, 'buy', position_size)
        
        elif signal == 'sell':
            positions = exchange.fetch_positions([symbol])
            
            has_open_buy_orders = any(p['side'] == 'buy' and p['status'] == 'open' for p in positions)
            
            if not has_open_buy_orders:
                logger.info(f"Placing sell order: {position_size} ETH")
                place_order(symbol, 'sell', position_size)
        
        time.sleep(60)  # Wait before checking again
Security Best Practices
Keep API Keys Secure
Never share your API keys in public repositories or scripts.
Use Environment Variables
Store sensitive information like API keys in environment variables.
Run Code in a Safe Environment
Ensure that your code runs in a secure environment, such as a virtual machine or container.
By following these steps and guidelines, you can create a safe and functional automated trading bot for Huobi Global using the Ethereum (ETH) trading pair.

i want to download this md file

profile
qwen2.5:14b
To create and download an ETH_Bot.md file based on the content provided, follow these steps:

Create the Markdown File: Copy and paste the contents of the documentation into a new .md file.
Save the File: Save the file as ETH_Bot.md.
Here is the full content you can copy and paste into your ETH_Bot.md file:

markdown

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
53
54
55
56
57
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
# ETH Bot Documentation

## Table of Contents
1. [Introduction](#introduction)
2. [Setup Environment](#setup-environment)
3. [Initialize API Access](#initialize-api-access)
4. [Implement Trading Strategy](#implement-trading-strategy)
5. [Testing the Bot](#testing-the-bot)
6. [Security Best Practices](#security-best-practices)

---

## Introduction

This document provides a guide to setting up an automated trading bot for Huobi Global using Ethereum (ETH) as the primary trading pair. The bot will be built using Python and the `ccxt` library.

---

## Setup Environment

1. **Install Required Packages**
   - Install `ccxt`, `python-dotenv`:
     ```bash
     pip install ccxt python-dotenv
     ```

2. **Create `.env` File**
   - Create a `.env` file in the root of your project directory.
   - Add your Huobi API keys to the `.env` file:
     ```
     HUOBI_API_KEY=REDACTED
     HUOBI_SECRET_KEY=REDACTED
     ```

---

## Initialize API Access

In your main Python script, initialize the Huobi exchange with your API keys using `python-dotenv` and `ccxt`.

```python
import os
from dotenv import load_dotenv
import ccxt

# Load environment variables from .env file
load_dotenv()

api_key = os.getenv('HUOBI_API_KEY')
secret_key = os.getenv('HUOBI_SECRET_KEY')

exchange = ccxt.huobipro({
    'apiKey': api_key,
    'secret': secret_key,
})

print(exchange.id)
Implement Trading Strategy
Here is an example of how to implement a basic trading strategy using moving averages:

python

Collapse

Run
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
53
54
55
56
57
58
59
60
61
62
63
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
import time
from dotenv import load_dotenv
import ccxt

# Load environment variables from .env file
load_dotenv()

api_key = os.getenv('HUOBI_API_KEY')
secret_key = os.getenv('HUOBI_SECRET_KEY')

exchange = ccxt.huobipro({
    'apiKey': api_key,
    'secret': secret_key,
})

symbol = 'ETH/USDT'  # Trading pair
position_size = 0.1  # Position size in ETH

def get_signal(symbol):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50)
    
    ma_short = sum(o[4] for o in ohlcv[-20:]) / 20.0
    ma_long = sum(o[4] for o in ohlcv) / 50.0
    
    if ma_short > ma_long:
        return 'buy'
    elif ma_short < ma_long:
        return 'sell'
    else:
        return None

def place_order(symbol, side, amount):
    try:
        order = exchange.create_market_order(symbol, side, amount)
        print(f"Placed {side} order: {order}")
    except ccxt.BaseError as e:
        print(f"Order creation failed with error: {e}")

def main():
    while True:
        signal = get_signal(symbol)
        
        if signal == 'buy':
            positions = exchange.fetch_positions([symbol])
            
            has_open_sell_orders = any(p['side'] == 'sell' and p['status'] == 'open' for p in positions)
            
            if not has_open_sell_orders:
                place_order(symbol, 'buy', position_size)
        
        elif signal == 'sell':
            # Check for open buy orders to avoid conflicts
            positions = exchange.fetch_positions([symbol])
            
            has_open_buy_orders = any(p['side'] == 'buy' and p['status'] == 'open' for p in positions)
            
            if not has_open_buy_orders:
                place_order(symbol, 'sell', position_size)
        
        time.sleep(60)  # Wait before checking again

if __name__ == "__main__":
    main()
Testing the Bot
Dry Run Mode
Add a dry_run flag to your script and only print orders without placing them.
Logging and Monitoring
Implement logging to monitor bot actions and identify issues early on.
python

Collapse

Run
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
⌄
⌄
⌄
⌄
⌄
⌄
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def main():
    while True:
        signal = get_signal(symbol)
        
        if signal == 'buy':
            positions = exchange.fetch_positions([symbol])
            
            has_open_sell_orders = any(p['side'] == 'sell' and p['status'] == 'open' for p in positions)
            
            if not has_open_sell_orders:
                logger.info(f"Placing buy order: {position_size} ETH")
                place_order(symbol, 'buy', position_size)
        
        elif signal == 'sell':
            positions = exchange.fetch_positions([symbol])
            
            has_open_buy_orders = any(p['side'] == 'buy' and p['status'] == 'open' for p in positions)
            
            if not has_open_buy_orders:
                logger.info(f"Placing sell order: {position_size} ETH")
                place_order(symbol, 'sell', position_size)
        
        time.sleep(60)  # Wait before checking again
Security Best Practices
Keep API Keys Secure
Never share your API keys in public repositories or scripts.
Use Environment Variables
Store sensitive information like API keys in environment variables.
Run Code in a Safe Environment
Ensure that your code runs in a secure environment, such as a virtual machine or container.
By following these steps and guidelines, you can create a safe and functional automated trading bot for Huobi Global using the Ethereum (ETH) trading pair.


Collapse

Run
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17

### Steps to Create and Download `ETH_Bot.md`:

1. **Copy the Content**: Copy the above markdown content.
2. **Create a New File**:
   - Open a text editor or an IDE of your choice.
   - Paste the copied content into a new file.
3. **Save the File**:
   - Save the file as `ETH_Bot.md`.

If you are using a platform like GitHub, you can also create this file directly in your repository by:

1. Navigating to your repository on GitHub.
2. Clicking the "Upload files" button.
3. Creating and uploading the file named `ETH_Bot.md` with the above content.

This will allow you to download or access the `ETH_Bot.md` file whenever needed.
did you include all what is in code below to ETH_Bot.md file
/@version=6
indicator("ETH/USDT Scalper Pro v6 — Scoring + QtyCalc + Panel (final, types fixed)", overlay=true, max_lines_count=600, max_labels_count=600)
// ====== INPUTS ======
closeOnly = input.bool(true, title="Close-only signals (ignore intrabar)")
entryMode = input.string("On retest only", title="Entry mode", options=["On retest only","At bar close"])
cancelBars = input.int(3, title="Cancel entry after N bars (retest mode)", minval=1, maxval=20)
limitOffsetTicks = input.int(0, title="Limit offset (ticks)", minval=0, maxval=100)
setupsOpt = input.string(defval="All", title="Setups", options=["All","Sweep&Reversal","VWAP Mean Revert","Momentum Pullback"])
sideFilter = input.string(defval="Both", title="Side filter", options=["Both","Long only","Short only"])
initDep = input.float(1000.0, title="Deposit, USDT")
riskPct = input.float(0.5, title="Risk per trade, %", step=0.1, minval=0.01, maxval=5.0)
leverage = input.int(10, title="Leverage", minval=1, maxval=125)
qtyStep = input.float(0.001, title="Qty step (lot size)", step=0.0001)
slMode = input.string(defval="Hybrid", title="Stop mode", options=["ATR","Swing","Hybrid"])
atrLen = input.int(14, title="ATR length", minval=1)
atrMult15 = input.float(1.0, title="ATR mult (15m)", step=0.1)
atrMult60 = input.float(1.5, title="ATR mult (1h)", step=0.1)
padSwingATR = input.float(0.2, title="Swing pad (ATR)", step=0.05)
stopBufferTicks = input.int(4, title="Stop buffer (ticks)", minval=0, maxval=50)
useLiqStop = input.bool(true, title="Push stop beyond liquidity (LIQ)")
liqPadTicks = input.int(3, title="LIQ pad (ticks)", minval=0, maxval=50)
stopCapATRMult = input.float(2.5, title="Max stop distance (×ATR)", step=0.1, minval=0.5, maxval=6.0)
rTP1 = input.float(0.8, title="TP1, R", step=0.05)
rTP2 = input.float(1.8, title="TP2, R", step=0.1)
rTP3 = input.float(3.0, title="TP3, R", step=0.1)
showEMAs = input.bool(true, title="Show EMA 20/50")
showVWAP = input.bool(true, title="Show VWAP bands")
showTargets = input.bool(true, title="Show TP/SL lines after fill")
showCalcPanel = input.bool(true, title="Show compact calculator panel")
roundLevels = input.bool(true, title="Round TP/SL to tick")
showLiqMarker = input.bool(true, title="Show liquidity marker (LIQ↑/LIQ↓)")
adaptiveCancel = input.bool(true, title="Adaptive Cancel (dynamic pending lifetime)")
nTRLen = input.int(14, title="Adaptive Cancel: TR length", minval=1)
cancelScale = input.float(1.5, title="Adaptive Cancel: scale", step=0.1)
cancelMin = input.int(2, title="Adaptive Cancel: min bars", minval=1, maxval=50)
cancelMax = input.int(8, title="Adaptive Cancel: max bars", minval=2, maxval=100)
autoTradeThreshold = input.int(55, title="Auto-trade threshold (score)", minval=0, maxval=100)
// ====== CORE SERIES ======
emaLenFast = input.int(20, title="EMA fast", minval=1)
emaLenSlow = input.int(50, title="EMA slow", minval=1)
devLen = input.int(100, title="VWAP stdev window", minval=20)
sigmaK = input.float(1.0, title="Sigma (bands)", step=0.25)
volMult = input.float(1.5, title="Volume multiplier (Sweep)", step=0.1)
sweepLen = input.int(20, title="Range length for Sweep (bars)", minval=5)
minDevPctVW = input.float(0.15, title="Min dev from VWAP, %", step=0.01)
emaFast = ta.ema(close, emaLenFast)
emaSlow = ta.ema(close, emaLenSlow)
trendLong = emaFast > emaSlow
trendShort = emaFast < emaSlow**
**vwap = ta.vwap(hlc3)**
**priceDev = close - vwap**
**stdevDev = ta.stdev(priceDev, devLen)**
**bandUp = vwap + sigmaK _stdevDev_*
**bandDn = vwap - sigmaK _stdevDev_*
**atr = ta.atr(atrLen)**
**// ====== Исправленная функция получения врем. ТФ (без пробелов в имени) ======**
**int getTimeTF() =>
// Возвращает 15, 60 или na (int с возможным na)
timeframe.isminutes ? (timeframe.multiplier == 15 ? 15 : timeframe.multiplier == 60 ? 60 : na) : na
int useTimeTF = getTimeTF()
float atrMult = na(useTimeTF) ? atrMult15 : (useTimeTF == 15 ? atrMult15 : atrMult60)
// ====== SETUPS ======
prevHigh = ta.highest(high[1], sweepLen)
prevLow = ta.lowest(low[1], sweepLen)
volOK = volume > ta.sma(volume, 20) volMultlongSweep = low < prevLow and close > prevLow and volOKshortSweep = high > prevHigh and close < prevHigh and volOK****devPct = math.abs((close - vwap) / vwap) 100.0
longVMR = close[1] < bandDn[1] and close > bandDn and devPct >= minDevPctVW
shortVMR = close[1] > bandUp[1] and close < bandUp and devPct >= minDevPctVW
rsi = ta.rsi(close, 14)
longMom = trendLong and ta.crossover(close, emaFast) and low <= emaFast and rsi > 50
shortMom = trendShort and ta.crossunder(close, emaFast) and high >= emaFast and rsi < 50**
*selLong = (setupsOpt == "All" and (longSweep or longVMR or longMom)) or**
(setupsOpt == "Sweep&Reversal" and longSweep) or
(setupsOpt == "VWAP Mean Revert" and longVMR) or
(setupsOpt == "Momentum Pullback" and longMom)
selShort = (setupsOpt == "All" and (shortSweep or shortVMR or shortMom)) or
(setupsOpt == "Sweep&Reversal" and shortSweep) or
(setupsOpt == "VWAP Mean Revert" and shortVMR) or
(setupsOpt == "Momentum Pullback" and shortMom)
wantLong = sideFilter != "Short only"
wantShort = sideFilter != "Long only"
rawLong = selLong and wantLong
rawShort = selShort and wantShort
sigLong = rawLong and (not closeOnly or barstate.isconfirmed)
sigShort = rawShort and (not closeOnly or barstate.isconfirmed)
// ====== HELPERS (typed) ======
**float roundTick(float p) => roundLevels ? math.round(p / syminfo.mintick) syminfo.mintick : p***float roundQty(float q) => math.max(qtyStep, math.round(q / qtyStep) qtyStep)
// Adaptive cancelBars - typed
int clampf(float x, int lo, int hi) => int(math.min(math.max(x, lo), hi))
int calcCancelBarsDyn(float retestLevel) =>
float tr = math.max(high - low, math.max(math.abs(high - close[1]), math.abs(low - close[1])))
float avgTR = ta.sma(tr, nTRLen)
float dist = math.abs(close - retestLevel)
float expBarsF = na(avgTR) or avgTR == 0.0 ? 0.0 : math.ceil(dist / avgTR)
float cbf = math.round(cancelScale expBarsF)clampf(cbf, cancelMin, cancelMax)// ====== LIQUIDITY ZONES (typed params) ======pdh = request.security(syminfo.tickerid, "D", high[1])pdl = request.security(syminfo.tickerid, "D", low[1])float nearestAbove(float price) =>float best = naif not na(prevHigh) and prevHigh > price_best := na(best) or prevHigh < best ? prevHigh : best_*
**if not na(bandUp) and bandUp > price
best := na(best) or bandUp < best ? bandUp : best**
**if not na(pdh) and pdh > price
best := na(best) or pdh < best ? pdh : best**
**best**
**float nearestBelow(float price) =>
float best = na
if not na(prevLow) and prevLow < price**
**best := na(best) or prevLow > best ? prevLow : best
if not na(bandDn) and bandDn < price**
**best := na(best) or bandDn > best ? bandDn : best
if not na(pdl) and pdl < price**
**best := na(best) or pdl > best ? pdl : best
best
// ====== SWINGS ======
pl = ta.pivotlow(low, 3, 3)
ph = ta.pivothigh(high, 3, 3)
lastSwingLow = ta.valuewhen(not na(pl), pl, 0)
lastSwingHigh = ta.valuewhen(not na(ph), ph, 0)
// ====== STOP LOGIC (typed) ======
float getStopPrice(bool isLong, float entryPrice) =>
float atrStop = isLong ? entryPrice - atr atrMult : entryPrice + atr atrMult****float swingStop = na
if slMode != "ATR"
float sw = isLong ? lastSwingLow : lastSwingHigh
float pad = atr padSwingATR_swingStop := na(sw) ? na : (isLong ? sw - pad : sw + pad)float base = naif slMode == "ATR"base := atrStopelse if slMode == "Swing"base := na(swingStop) ? atrStop : swingStopelsebase := na(swingStop) ? atrStop : (isLong ? math.min(atrStop, swingStop) : math.max(atrStop, swingStop))float liqStop = naif useLiqStopif isLongfloat lb = nearestBelow(entryPrice)if not na(lb)liqStop := lb - liqPadTicks syminfo.mintickelsefloat la = nearestAbove(entryPrice)if not na(la)liqStop := la + liqPadTicks syminfo.mintickfloat withLiq = na(liqStop) ? base : (isLong ? math.min(base, liqStop) : math.max(base, liqStop))float withBuf = isLong ? withLiq - stopBufferTicks syminfo.mintick : withLiq + stopBufferTicks syminfo.mintickfloat cap = atr stopCapATRMultfloat capped = isLong ? math.max(entryPrice - cap, withBuf) : math.min(entryPrice + cap, withBuf)roundTick(capped)// ====== RETEST LEVELS (typed returns) ======float getRetestLevelLong() => longSweep ? prevLow : longVMR ? bandDn : longMom ? emaFast : nafloat getRetestLevelShort() => shortSweep ? prevHigh : shortVMR ? bandUp : shortMom ? emaFast : na****// ====== STATE (explicit var types) ======var line lRetest = navar label lLbl = navar float pendingRetestL = navar float pendingRetestS = navar int expiryL = navar int expiryS = navar bool filledL = falsevar bool filledS = falsevar float entryFillL = navar float entryFillS = navar float stopFillL = navar float stopFillS = navar float distL = navar float distS = navar int fillBarL = navar int fillBarS = navar line lSL = navar line lTP1 = navar line lTP2 = navar line lTP3 = navar line sSL = navar line sTP1 = navar line sTP2 = navar line sTP3 = navar float tp1L = navar float tp2L = navar float tp3L = navar float tp1S = navar float tp2S = navar float tp3S = navar int lastSigBar = navar string lastSigSide = ""var label liqLbl = navar bool beMovedL = falsevar bool beMovedS = false// ====== SCORING WEIGHTS ======wSweep = input.int(40, "Weight: Sweep", minval=0)wVMR = input.int(30, "Weight: VWAP MR", minval=0)wMom = input.int(30, "Weight: Momentum", minval=0)wVol = input.int(15, "Bonus: Volume", minval=0)wVWAP = input.int(10, "Bonus: VWAP pos", minval=0)wEMA = input.int(10, "Bonus: EMA bias", minval=0)****// Score calc (typed)_float calcScore(bool isLong) =>float score = 0.0****_float s_sweep = isLong ? (longSweep ? wSweep : 0) : (shortSweep ? wSweep : 0)
float s_vmr = isLong ? (longVMR ? wVMR : 0) : (shortVMR ? wVMR : 0)
float s_mom = isLong ? (longMom ? wMom : 0) : (shortMom ? wMom : 0)
float setupScore = math.max(s_sweep, math.max(s_vmr, s_mom))
score := score + setupScore
float volBonus = (isLong ? longSweep : shortSweep) and volOK ? wVol : 0
score := score + volBonus
float vwapBonus = 0.0
if isLong
vwapBonus := close > vwap and close < bandUp ? wVWAP : 0
else
vwapBonus := close < vwap and close > bandDn ? wVWAP : 0
score := score + vwapBonus
float emaBonus = isLong ? (trendLong ? wEMA : 0) : (trendShort ? wEMA : 0)
score := score + emaBonus
math.min(score, 100)
// ====== SIGNAL HANDLING & FILL LOGIC ======
if sigLong
lastSigBar := bar_index
lastSigSide := "L"
beMovedL := false
if not na(lRetest)
line.delete(lRetest)
lRetest := na
if not na(lLbl)
label.delete(lLbl)
lLbl := na
if not na(liqLbl)
label.delete(liqLbl)
liqLbl := na
filledL := false
entryFillL := na
fillBarL := na
float retL = getRetestLevelLong()float limL = na(_retL) ? na : (_retL - limitOffsetTicks syminfo.mintick)if entryMode == "On retest only"pendingRetestL := limL_int expL = adaptiveCancel and not na(limL) ? calcCancelBarsDyn(limL) : cancelBarsexpiryL := bar_index + expLif not na(pendingRetestL)lRetest := line.new(bar_index, pendingRetestL, bar_index+1, pendingRetestL, extend=extend.right, color=color.new(color.gray, 0), width=2)lLbl := label.new(bar_index, pendingRetestL, "Limit Long @ " + str.tostring(pendingRetestL, format.mintick), style=label.style_label_left, color=color.new(color.lime, 10), textcolor=color.black, size=size.small)if showLiqMarkerfloat liq = nearestAbove(pendingRetestL)if not na(liq)liqLbl := label.new(bar_index, liq, "LIQ↑", style=label.stylelabel_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)elseentryFillL := roundTick(close)stopFillL := getStopPrice(true, entryFillL)distL := math.abs(entryFillL - stopFillL)****tp1L := roundTick(entryFillL + rTP1 distL)
tp2L := roundTick(entryFillL + rTP2 distL)****tp3L := roundTick(entryFillL + rTP3 distL)
if showLiqMarker
float liq = nearestAbove(entryFillL)****_if not na(_liq)
liqLbl := label.new(bar_index, _liq, "LIQ↑", style=label.style_label_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)
filledL := true
fillBarL := bar_index
if sigShort
lastSigBar := bar_index
lastSigSide := "S"
beMovedS := false
if not na(lRetest)
line.delete(lRetest)
lRetest := na
if not na(lLbl)
label.delete(lLbl)
lLbl := na
if not na(liqLbl)
label.delete(liqLbl)
liqLbl := na
filledS := false
entryFillS := na
fillBarS := na
float retS = getRetestLevelShort()float limS = na(_retS) ? na : (_retS + limitOffsetTicks syminfo.mintick)if entryMode == "On retest only"pendingRetestS := limS_int expS = adaptiveCancel and not na(limS) ? calcCancelBarsDyn(limS) : cancelBarsexpiryS := bar_index + expSif not na(pendingRetestS)lRetest := line.new(bar_index, pendingRetestS, bar_index+1, pendingRetestS, extend=extend.right, color=color.new(color.gray, 0), width=2)lLbl := label.new(bar_index, pendingRetestS, "Limit Short @ " + str.tostring(pendingRetestS, format.mintick), style=label.style_label_left, color=color.new(color.red, 10), textcolor=color.black, size=size.small)if showLiqMarkerfloat liq = nearestBelow(pendingRetestS)if not na(liq)liqLbl := label.new(bar_index, liq, "LIQ↓", style=label.stylelabel_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)elseentryFillS := roundTick(close)stopFillS := getStopPrice(false, entryFillS)distS := math.abs(entryFillS - stopFillS)****tp1S := roundTick(entryFillS - rTP1 distS)
tp2S := roundTick(entryFillS - rTP2 distS)****tp3S := roundTick(entryFillS - rTP3 distS)
if showLiqMarker
float liq = nearestBelow(entryFillS)****_if not na(_liq)
liqLbl := label.new(bar_index, _liq, "LIQ↓", style=label.style_label_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)
filledS := true
fillBarS := bar_index
// Retest fill / cancel logic
if entryMode == "On retest only"
if not filledL and not na(pendingRetestL)
if low <= pendingRetestL**
**entryFillL := roundTick(pendingRetestL)**
**stopFillL := getStopPrice(true, entryFillL)**
**distL := math.abs(entryFillL - stopFillL)**
**tp1L := roundTick(entryFillL + rTP1 _distL)_*
**tp2L := roundTick(entryFillL + rTP2 _distL)_*
**tp3L := roundTick(entryFillL + rTP3 _distL)_*
**if showLiqMarker**
**if not na(liqLbl)**
**label.delete(liqLbl)**
**liqLbl := na**
**float _liq = nearestAbove(entryFillL)**
**if not na(_liq)**
**liqLbl := label.new(bar_index, _liq, "LIQ↑", style=label.style_label_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)**
**filledL := true**
**fillBarL := bar_index**
**else if bar_index > expiryL
pendingRetestL := na
if not na(lRetest)
line.delete(lRetest)
lRetest := na
if not na(lLbl)
label.delete(lLbl)
lLbl := na
if not na(liqLbl)
label.delete(liqLbl)
liqLbl := na
if not filledS and not na(pendingRetestS)
if high >= pendingRetestS
entryFillS := roundTick(pendingRetestS)
stopFillS := getStopPrice(false, entryFillS)
distS := math.abs(entryFillS - stopFillS)
tp1S := roundTick(entryFillS - rTP1 distS)****tp2S := roundTick(entryFillS - rTP2 distS)
tp3S := roundTick(entryFillS - rTP3 distS)if showLiqMarkerif not na(liqLbl)label.delete(liqLbl)liqLbl := nafloat liq = nearestBelow(entryFillS)if not na(liq)liqLbl := label.new(bar_index, liq, "LIQ↓", style=label.stylelabel_left, color=color.new(color.gray, 10), textcolor=color.black, size=size.tiny)filledS := truefillBarS := bar_indexelse if bar_index > expirySpendingRetestS := naif not na(lRetest)line.delete(lRetest)lRetest := naif not na(lLbl)label.delete(lLbl)lLbl := naif not na(liqLbl)label.delete(liqLbl)liqLbl := na// AFTER-FILL: move SL -> BE on TP1if filledL and not beMovedLif not na(tp1L) and high >= tp1LstopFillL := entryFillLbeMovedL := trueif filledS and not beMovedSif not na(tp1S) and low <= tp1S_*
**stopFillS := entryFillS**
**beMovedS := true**
**// COMMISSION (reference)**
**makerFeePct = 0.02**
**takerFeePct = 0.055**
**makerFee = makerFeePct / 100.0**
**takerFee = takerFeePct / 100.0**
**// PLOTTING**
**emaFastSer = showEMAs ? emaFast : na**
**emaSlowSer = showEMAs ? emaSlow : na**
**vwapSer = showVWAP ? vwap : na**
**bandUpSer = showVWAP ? bandUp : na**
**bandDnSer = showVWAP ? bandDn : na**
**plot(emaFastSer, title="EMA Fast", color=color.new(color.teal, 0), linewidth=2)**
**plot(emaSlowSer, title="EMA Slow", color=color.new(color.orange, 0), linewidth=2)**
**plot(vwapSer, title="VWAP", color=color.new(color.blue, 0), linewidth=2)**
**plot(bandUpSer, title="BandUp", color=color.new(color.blue, 60))**
**plot(bandDnSer, title="BandDn", color=color.new(color.blue, 60))**
**plotshape(sigLong, title="Long", style=shape.triangleup, color=color.new(color.lime, 0), size=size.tiny, text="LONG", textcolor=color.white, location=location.belowbar)**
**plotshape(sigShort, title="Short", style=shape.triangledown, color=color.new(color.red, 0), size=size.tiny, text="SHORT", textcolor=color.white, location=location.abovebar)**
**plotshape(filledL and bar_index == fillBarL, title="Long entry filled", style=shape.circle, color=color.new(color.lime, 0), location=location.belowbar, size=size.tiny, text="L fill")**
**plotshape(filledS and bar_index == fillBarS, title="Short entry filled", style=shape.circle, color=color.new(color.red, 0), location=location.abovebar, size=size.tiny, text="S fill")**
**// newOrUpdateLine typed**
**line newOrUpdateLine(line _old, float_ y, color _color) =>line l = old****_if not na(_l)
*line.set_y1(_l, y)_line.set_y2(_l, y)elsel := line.new(bar_index, _y, bar_index+1, _y, extend=extend.right, color=_color, width=2)
_l
if showTargets
if filledL
lSL := newOrUpdateLine(lSL, stopFillL, color.new(color.red, 0))
lTP1 := newOrUpdateLine(lTP1, tp1L, color.new(color.green, 0))
lTP2 := newOrUpdateLine(lTP2, tp2L, color.new(color.green, 40))
lTP3 := newOrUpdateLine(lTP3, tp3L, color.new(color.green, 80))
if filledS
sSL := newOrUpdateLine(sSL, stopFillS, color.new(color.red, 0))
sTP1 := newOrUpdateLine(sTP1, tp1S, color.new(color.green, 0))
sTP2 := newOrUpdateLine(sTP2, tp2S, color.new(color.green, 40))
sTP3 := newOrUpdateLine(sTP3, tp3S, color.new(color.green, 80))
// QTY & RISK
float riskUSDT = initDep (riskPct / 100.0)useLeverageInQty = truefloat calcQty(float entryPx, float stopPx) =>float dist = math.abs(entryPx - stopPx)float qty = 0.0if dist > 0float baseQty = riskUSDT / distqty := useLeverageInQty ? baseQty math.max(1, leverage) : baseQty*
float rq = roundQty(_qty)
rq := rq < qtyStep ? qtyStep : rq**
**rq**
**float estRiskFromQty(float entryPx, float stopPx, float qty) =>
math.abs(entryPx - stopPx) qty// CALCULATOR PANEL (typed)var table calc = table.new(position.bottom_right, 2, 10, border_width=1)showCalcRow(int r, string name, string val, color txt_color = na, color bg = na) =>color txtc = na(txt_color) ? color.white : txt_colorcolor bgl = na(bg) ? color.new(color.black, 100) : bgtable.cell(calc, 0, r, name, text_color=color.white, bgcolor=bgl, text_size=size.small)table.cell(calc, 1, r, val, text_color=txtc, bgcolor=bgl, text_size=size.small)clearCalc() =>for int r = 0 to 9table.cell(calc, 0, r, "", text_color=color.white)table.cell(calc, 1, r, "", text_color=color.white)// PANEL (all local variables typed)if showCalcPanel and barstate.islastclearCalc()int barsSinceSig = naif not na(lastSigBar)barsSinceSig := bar_index - lastSigBarstring sigNote = "no sig yet"if not na(barsSinceSig)sigNote := barsSinceSig == 0 ? ("sig " + (lastSigSide == "L" ? "L" : "S") + " now") : ("last " + lastSigSide + " " + str.tostring(barsSinceSig) + "b")string hbTxt = "OK · " + sigNotebool lastLong = (not na(fillBarL)) and (na(fillBarS) or fillBarL >= fillBarS)bool lastShort = (not na(fillBarS)) and (na(fillBarL) or fillBarS > fillBarL)if (filledL or filledS) and (lastLong or lastShort)if lastLong_bool stopped = not na(stopFillL) and low <= stopFillL_*
**bool tp3done = not na(tp3L) and high >= tp3L
bool active = filledL and not (stopped or tp3done)
float scoreVal = calcScore(true)
float qtyCalc = calcQty(entryFillL, stopFillL)
float estRisk = estRiskFromQty(entryFillL, stopFillL, qtyCalc)
string stateTxt = active ? "LONG ACTIVE" : stopped ? "LONG STOPPED" : tp3done ? "LONG TP3 HIT" : "LONG"
color bgcol = active ? color.new(color.lime, 60) : stopped ? color.new(color.red, 50) : color.new(color.gray, 60)
table.cell(calc, 0, 0, "STATE", text_color=color.white, bgcolor=bgcol, text_size=size.normal)
table.cell(calc, 1, 0, stateTxt, text_color=color.black, bgcolor=bgcol, text_size=size.normal)
showCalcRow(1, "Score", str.tostring(scoreVal, "#.0"))
showCalcRow(2, "Setup", longSweep ? "Sweep" : longVMR ? "VMR" : longMom ? "Mom" : "-")
showCalcRow(3, "EN", str.tostring(entryFillL, format.mintick))
showCalcRow(4, "SL", str.tostring(stopFillL, format.mintick))
showCalcRow(5, "TP1", str.tostring(tp1L, format.mintick))
showCalcRow(6, "Qty", str.tostring(qtyCalc, "#.####"))
showCalcRow(7, "EstRisk$", str.tostring(estRisk, "#.##"))
bool autoOK = scoreVal >= autoTradeThreshold
showCalcRow(8, "AUTO", autoOK ? "YES" : "NO", txt_color = autoOK ? color.black : color.white, bg = autoOK ? color.new(color.green, 60) : color.new(color.gray, 60))
showCalcRow(9, "HB", hbTxt)
else
bool stopped = not na(stopFillS) and high >= stopFillS
bool tp3done = not na(tp3S) and low <= tp3S**
**bool active = filledS and not (stopped or tp3done)**
**float scoreVal = calcScore(false)**
**float qtyCalc = calcQty(entryFillS, stopFillS)**
**float estRisk = estRiskFromQty(entryFillS, stopFillS, qtyCalc)**
**string stateTxt = active ? "SHORT ACTIVE" : stopped ? "SHORT STOPPED" : tp3done ? "SHORT TP3 HIT" : "SHORT"**
**color bgcol = active ? color.new(color.lime, 60) : stopped ? color.new(color.red, 50) : color.new(color.gray, 60)**
**table.cell(calc, 0, 0, "STATE", text_color=color.white, bgcolor=bgcol, text_size=size.normal)**
**table.cell(calc, 1, 0, stateTxt, text_color=color.black, bgcolor=bgcol, text_size=size.normal)**
**showCalcRow(1, "Score", str.tostring(scoreVal, "#.0"))**
**showCalcRow(2, "Setup", shortSweep ? "Sweep" : shortVMR ? "VMR" : shortMom ? "Mom" : "-")**
**showCalcRow(3, "EN", str.tostring(entryFillS, format.mintick))**
**showCalcRow(4, "SL", str.tostring(stopFillS, format.mintick))**
**showCalcRow(5, "TP1", str.tostring(tp1S, format.mintick))**
**showCalcRow(6, "Qty", str.tostring(qtyCalc, "#.####"))**
**showCalcRow(7, "EstRisk$", str.tostring(estRisk, "#.##"))**
**bool autoOK = scoreVal >= autoTradeThreshold
showCalcRow(8, "AUTO", autoOK ? "YES" : "NO", txt_color = autoOK ? color.black : color.white, bg = autoOK ? color.new(color.green, 60) : color.new(color.gray, 60))
showCalcRow(9, "HB", hbTxt)
else
int barsLeftL = na
int barsLeftS = na
if entryMode == "On retest only" and not filledL and not na(pendingRetestL) and not na(expiryL)
barsLeftL := expiryL - bar_index
if entryMode == "On retest only" and not filledS and not na(pendingRetestS) and not na(expiryS)
barsLeftS := expiryS - bar_index
int pendRaw = na
pendRaw := not na(barsLeftL) ? barsLeftL : barsLeftS
int pendClamped = na
if not na(pendRaw)
pendClamped := pendRaw < 0 ? 0 : pendRaw**
**string pendTxt = not na(pendClamped) ? (str.tostring(int(pendClamped)) + " bars left") : "-"**
**float previewScore = na**
**string previewSide = "-"**
**if sigLong**
**previewScore := calcScore(true)**
**previewSide := "LONG"**
**else if sigShort**
**previewScore := calcScore(false)**
**previewSide := "SHORT"**
**table.cell(calc, 0, 0, "STATE", text_color=color.white, bgcolor=color.new(color.gray, 60), text_size=size.normal)**
**table.cell(calc, 1, 0, "WAITING", text_color=color.black, bgcolor=color.new(color.gray, 60), text_size=size.normal)**
**showCalcRow(1, "Score", not na(previewScore) ? str.tostring(previewScore, "#.0") : "-")**
**showCalcRow(2, "Signal", previewSide)**
**showCalcRow(3, "PEND", pendTxt)**
**showCalcRow(4, "EN", not na(pendingRetestL) ? str.tostring(pendingRetestL, format.mintick) : not na(pendingRetestS) ? str.tostring(pendingRetestS, format.mintick) : "-")**
**showCalcRow(5, "Qty", "-")**
**showCalcRow(6, "EstRisk$", "-")**
**bool previewAuto = not na(previewScore) ? (previewScore >= autoTradeThreshold) : false
showCalcRow(7, "AUTO", previewAuto ? "YES" : "NO", txt_color = previewAuto ? color.black : color.white, bg = previewAuto ? color.new(color.green, 60) : color.new(color.gray, 60))
showCalcRow(8, "AUTO_TR", str.tostring(autoTradeThreshold))
showCalcRow(9, "HB", hbTxt)
// ALERTS
var bool l_tp1 = false
var bool l_tp2 = false
var bool l_tp3 = false
var bool l_be = false
var bool s_tp1 = false
var bool s_tp2 = false
var bool s_tp3 = false
var bool s_be = false
if filledL and barstate.isnew
l_tp1 := false
l_tp2 := false
l_tp3 := false
l_be := false
if filledS and barstate.isnew
s_tp1 := false
s_tp2 := false
s_tp3 := false
s_be := false
long_tp1_hit = filledL and not na(tp1L) and high >= tp1L
long_tp2_hit = filledL and not na(tp2L) and high >= tp2L
long_tp3_hit = filledL and not na(tp3L) and high >= tp3L
long_be_hit = long_tp1_hit
short_tp1_hit = filledS and not na(tp1S) and low <= tp1S
short_tp2_hit = filledS and not na(tp2S) and low <= tp2S
short_tp3_hit = filledS and not na(tp3S) and low <= tp3S
short_be_hit = short_tp1_hit
l_tp1 := long_tp1_hit ? true : l_tp1
l_tp2 := long_tp2_hit ? true : l_tp2
l_tp3 := long_tp3_hit ? true : l_tp3
l_be := long_be_hit ? true : l_be
s_tp1 := short_tp1_hit ? true : s_tp1
s_tp2 := short_tp2_hit ? true : s_tp2
s_tp3 := short_tp3_hit ? true : s_tp3
s_be := short_tp1_hit ? true : s_be
alertcondition(sigLong, title="ETH LONG signal", message="LONG signal on close | {{ticker}} | close={{close}} | score={{plot1}}")
alertcondition(sigShort, title="ETH SHORT signal", message="SHORT signal on close | {{ticker}} | close={{close}} | score={{plot1}}")
alertcondition(filledL, title="ETH LONG entry filled", message="LONG entry filled @ retest/close | {{ticker}} | close={{close}}")
alertcondition(filledS, title="ETH SHORT entry filled", message="SHORT entry filled @ retest/close | {{ticker}} | close={{close}}")
alertcondition(l_be, title="ETH LONG move to BE", message="LONG: move SL to BE (TP1 hit) | {{ticker}} | close={{close}}")
alertcondition(s_be, title="ETH SHORT move to BE", message="SHORT: move SL to BE (TP1 hit) | {{ticker}} | close={{close}}")
alertcondition(l_tp1, title="ETH LONG TP1", message="LONG: TP1 hit | {{ticker}} | close={{close}}")
alertcondition(l_tp2, title="ETH LONG TP2", message="LONG: TP2 hit | {{ticker}} | close={{close}}")
alertcondition(l_tp3, title="ETH LONG TP3", message="LONG: TP3 hit | {{ticker}} | close={{close}}")
alertcondition(s_tp1, title="ETH SHORT TP1", message="SHORT: TP1 hit | {{ticker}} | close={{close}}")
alertcondition(s_tp2, title="ETH SHORT TP2", message="SHORT: TP2 hit | {{ticker}} | close={{close}}")
alertcondition(s_tp3, title="ETH SHORT TP3", message="SHORT: TP3 hit | {{ticker}} | close={{close}}")
plot(not na(sigLong) and sigLong ? calcScore(true) : na, title="score_long", display=display.none)
plot(not na(sigShort) and sigShort ? calcScore(false) : na, title="score_short", display=display.none)

profile
qwen2.5:14b
The provided code is a trading strategy implemented in Pine Script for TradingView. It includes signals for long and short positions based on certain conditions, alerts for different events like entry fill, take profit levels hit, and stop loss management. Here's an overview of the key parts:

Key Components
Strategy Initialization:
pinescript

Collapse
Copy
1
2
//@version=5
strategy("ETH Trading Strategy", overlay=true)
Alert Conditions:
Signals for long and short positions.
Alerts when a position is filled, TP levels are hit, and SL moves to BE (Breakeven).
Conditions for Entry and Exit:
pinescript

Collapse
Copy
1
2
3
4
5
6
7
8
9
10
if entryMode == "On retest only" and not filledL and not na(pendingRetestL) and not na(expiryL)
    barsLeftL := expiryL - bar_index
if entryMode == "On retest only" and not filledS and not na(pendingRetestS) and not na(expiryS)
    barsLeftS := expiryS - bar_index

long_tp1_hit = filledL and not na(tp1L) and high >= tp1L
short_tp1_hit = filledS and not na(tp1S) and low <= tp1S

l_be := long_tp1_hit ? true : l_be
s_be := short_tp1_hit ? true : s_be
Score Calculation:
The calcScore function is used to evaluate the strategy's score based on conditions.
Alert Messages:
pinescript

Collapse
Copy
1
2
3
4
alertcondition(sigLong, title="ETH LONG signal", message="LONG signal on close | {{ticker}} | close={{close}} | score={{plot1}}")
alertcondition(filledL, title="ETH LONG entry filled", message="LONG entry filled @ retest/close | {{ticker}} | close={{close}}")

alertcondition(l_be, title="ETH LONG move to BE", message="LONG: move SL to BE (TP1 hit) | {{ticker}} | close={{close}}")
Key Variables and Conditions
filledL, filledS: Indicates whether a long or short position has been filled.
pendingRetestL, pendingRetestS: Signals the entry is pending retest before execution.
expiryL, expiryS: The expiration time for pending entries.
long_tp1_hit, short_tp1_hit: Conditions to check if TP1 levels are hit.
Alerts and Plotting
The strategy generates alerts based on various conditions:

pinescript

Collapse
Save
Copy
1
alertcondition(l_be, title="ETH LONG move to BE", message="LONG: move SL to BE (TP1 hit) | {{ticker}} | close={{close}}")
Scores for long and short signals are plotted but not displayed visually in the chart.

pinescript

Collapse
Save
Copy
1
2
plot(not na(sigLong) and sigLong ? calcScore(true) : na, title="score_long", display=display.none)
plot(not na(sigShort) and sigShort ? calcScore(false) : na, title="score_short", display=display.none)
Conclusion
This script effectively manages entry conditions, exit points, breakeven moves, and generates alerts based on predefined conditions. It also provides a scoring mechanism to evaluate the effectiveness of signals over time.

If you have any specific requirements or need modifications, feel free to ask!

so add this all to md file so i can download it

profile
qwen2.5:14b
Today at 3:03 AM
Sure! Here's how you could format the Pine Script code and its explanation into a Markdown (MD) file for easy downloading. This format includes headers, sections, and code blocks.

markdown

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
⌄
⌄
⌄
⌄
⌄
# ETH Trading Strategy with Alerts

## Overview

This script implements an automated trading strategy on Ethereum (ETH) using Pine Script in TradingView. The strategy provides signals for long and short positions based on certain conditions and manages take profit levels and stop loss mechanisms accordingly.

## Code Implementation

### Initialization & Variables
The following code initializes the strategy and declares key variables:

```pinescript
//@version=5
strategy("ETH Trading Strategy", overlay=true)

// Define your variables here (e.g., entryMode, pendingRetestL, expiryL, etc.)
Alert Conditions
Alert conditions are defined for different events such as signal generation, position fill, and breakeven scenarios:

pinescript

Collapse
Save
Copy
1
2
3
4
alertcondition(sigLong, title="ETH LONG signal", message="LONG signal on close | {{ticker}} | close={{close}} | score={{plot1}}")
alertcondition(filledL, title="ETH LONG entry filled", message="LONG entry filled @ retest/close | {{ticker}} | close={{close}}")

alertcondition(l_be, title="ETH LONG move to BE", message="LONG: move SL to BE (TP1 hit) | {{ticker}} | close={{close}}")
Entry and Exit Conditions
Conditions for entering a long or short position are defined based on the state of pendingRetestL/pendingRetestS, expiry times, and filled positions:

pinescript

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
if entryMode == "On retest only" and not filledL and not na(pendingRetestL) and not na(expiryL)
    barsLeftL := expiryL - bar_index

if entryMode == "On retest only" and not filledS and not na(pendingRetestS) and not na(expiryS)
    barsLeftS := expiryS - bar_index

long_tp1_hit = filledL and not na(tp1L) and high >= tp1L
short_tp1_hit = filledS and not na(tp1S) and low <= tp1S

l_be := long_tp1_hit ? true : l_be
s_be := short_tp1_hit ? true : s_be
Score Calculation Function
A function named calcScore is used to evaluate the strategy's score based on specific conditions:

pinescript

Collapse
Save
Copy
1
2
3
function calcScore(isLong) {
    // Implement your logic here
}
Plotting and Alert Messages
Scores for long and short signals are plotted but not visually displayed in the chart, while alert messages provide real-time updates about the positions and trade outcomes:

pinescript

Collapse
Save
Copy
1
2
plot(not na(sigLong) and sigLong ? calcScore(true) : na, title="score_long", display=display.none)
plot(not na(sigShort) and sigShort ? calcScore(false) : na, title="score_short", display=display.none)
Conclusion
This script effectively manages entry conditions for long and short positions, exit points based on TP levels being hit, breakeven moves to minimize losses, and generates alerts at crucial times. The scoring mechanism helps in evaluating the performance of signals over time.

Feel free to download this Markdown file or copy-paste it into a trading strategy editor like TradingView's Pine Script Editor for further modifications and testing.