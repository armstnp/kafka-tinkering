package zonar.gprsd.writer.idleintervals;

import java.util.function.*;

public class FunctionalIdleRule implements IdleRule {
	private final Function<AssetPositionEvent, Boolean> eventCondition;
	private final BiFunction<AssetPositionEvent, AssetPositionEvent, Boolean> eventStopCondition;
	private final BiConsumer<IdleIntervalAccumulator, AssetPositionEvent> action;

	public FunctionalIdleRule(
			BiFunction<AssetPositionEvent, AssetPositionEvent, Boolean> condition,
			BiConsumer<IdleIntervalAccumulator, AssetPositionEvent> action) {

		this(null, condition, action);
	}

	public FunctionalIdleRule(
			Function<AssetPositionEvent, Boolean> condition,
			BiConsumer<IdleIntervalAccumulator, AssetPositionEvent> action) {

		this(condition, null, action);
	}

	private FunctionalIdleRule(
			Function<AssetPositionEvent, Boolean> eventCondition,
			BiFunction<AssetPositionEvent, AssetPositionEvent, Boolean> eventStopCondition,
			BiConsumer<IdleIntervalAccumulator, AssetPositionEvent> action) {

		this.eventCondition = eventCondition;
		this.eventStopCondition = eventStopCondition;
		this.action = action;
	}


	@Override
	public boolean apply(AssetPositionEvent event, IdleIntervalAccumulator accumulator) {
		AssetPositionEvent latestStop = accumulator.getLatestStop();

		Boolean ruleApplies;
		if(eventCondition != null) ruleApplies = eventCondition.apply(event);
		else ruleApplies = eventStopCondition.apply(latestStop, event);

		if(ruleApplies) {
			action.accept(accumulator, event);
		}

		return ruleApplies;
	}

	static void f() {
		IdleRule[] idleRules = {
				new FunctionalIdleRule(
						(event) -> event.isAssetColdStarted() && event.isMoving(),
						(accumulator, event) -> {
							accumulator.pushInterval(event);
							accumulator.setLatestStop(event);
						}
				),
				new FunctionalIdleRule(
						(stop, event) -> event.isAssetColdStarted() && event.isMoving(),
						IdleIntervalAccumulator::pushIntervalAndClear
				)
		};
	}
}
