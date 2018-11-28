//package LineDependencies;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class GPSLine extends GeoLine{
//	
//	private static final long serialVersionUID = 1L;
//	private Map<ShapeLine, Double> containedShapes;
//	
//	public GPSLine(String id){
//		super(id);
//		this.containedShapes = new HashMap<ShapeLine, Double>();
//	}
//	
//	@Override
//	public String toString() {
//		return "GPSLine [id=" + getId() + " shapeLine=" + getGeoLine().toString() + "]";
//	}
//	
//	public void addContainedShape(ShapeLine shapeLine) {
//		if (!containedShapes.containsKey(shapeLine)) {
//			this.containedShapes.put(shapeLine, 0.0);
//		}
//		Double currentDistance = this.containedShapes.get(shapeLine);
//		currentDistance += Double.valueOf(shapeLine.getRouteDistance());
//		
//	}
//	
//}
//
