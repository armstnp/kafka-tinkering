package zonar.gprsd.writer.idleintervals

import zonar.position_upload.PositionUpload

data class AssetPositionEvent(
        val account: String,
        val assetId: Long,
        val assetPositionEventId: Long,
        val timestamp: Long,
        val latitude: Double,
        val longitude: Double,
        val speed: Double,
        val isPowerOn: Boolean,
        val hasMotionStateChanged: Boolean,
        val isAssetColdStarted: Boolean
) {
    fun isMoving() = speed != 0.0
}

fun PositionUpload.toDomainEvents() =
        getPayload().map {
            AssetPositionEvent(
                    account = "", //TODO: Add account
                    assetId = deviceId,
                    assetPositionEventId = -1, //TODO: Add event ID
                    timestamp = getTs(),
                    latitude = it.getLatitude(),
                    longitude = it.getLongitude(),
                    speed = it.getSpeed(),
                    isPowerOn = (it.getStatus() and (1 shl 13)) != 0L,
                    hasMotionStateChanged = (it.getStatus() and (1 shl 10)) != 0L,
                    isAssetColdStarted = (it.getStatus() and (1 shl 14)) != 0L
            )
        }

data class IdleInterval(
        val account: String,
        val assetId: Long,
        val latitude: Double,
        val longitude: Double,
        val stopPowerOn: Boolean,
        val stopTimestamp: Long,
        val stopAssetPositionEventId: Long,
        val startTimestamp: Long,
        val startAssetPositionEventId: Long
)

fun buildInterval(stopEvent: AssetPositionEvent, startEvent: AssetPositionEvent) =
        IdleInterval(
                account = stopEvent.account,
                assetId = stopEvent.assetId,
                latitude = stopEvent.latitude,
                longitude = stopEvent.longitude,
                stopPowerOn = stopEvent.isPowerOn,
                stopTimestamp = stopEvent.timestamp,
                stopAssetPositionEventId = stopEvent.assetPositionEventId,
                startTimestamp = startEvent.timestamp,
                startAssetPositionEventId = startEvent.assetPositionEventId
        )

data class PotentialIdleStop(
        val account: String,
        val assetId: Long,
        val assetPositionEventId: Long,
        val timestamp: Long,
        val latitude: Double,
        val longitude: Double,
        val isPowerOn: Boolean
)