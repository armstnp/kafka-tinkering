package zonar.gprsd.writer.idleintervals;

public interface IdleRule {
	boolean apply(AssetPositionEvent event, IdleIntervalAccumulator accumulator);
}
