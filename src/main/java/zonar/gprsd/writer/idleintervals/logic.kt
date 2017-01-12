package zonar.gprsd.writer.idleintervals

import java.util.*

class IdleIntervalAccumulator(
        initialLatestStop: AssetPositionEvent?
){
    private val accumulatedIntervals = ArrayList<IdleInterval>()

    private var _latestStop: AssetPositionEvent? = initialLatestStop
    public val latestStop: AssetPositionEvent?
        get() = _latestStop

    val intervals: List<IdleInterval> = accumulatedIntervals

    fun clearLatestStop() {
        _latestStop = null
    }

    fun setLatestStop(stopEvent: AssetPositionEvent) {
        _latestStop = stopEvent
    }

    fun pushInterval(startEvent: AssetPositionEvent) {
        if (latestStop == null) throw IllegalStateException("Can't push interval with no latest stop")

        latestStop!!.let { stopEvent ->
            val interval = buildInterval(stopEvent, startEvent)
            accumulatedIntervals.add(interval)
        }
    }

    fun pushIntervalAndClear(startEvent: AssetPositionEvent) {
        pushInterval(startEvent)
        clearLatestStop()
    }
}

fun accumulatePosition(event: AssetPositionEvent) {

}