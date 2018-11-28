//package LineDependencies;
//
//import PointDependencies.ShapePoint;
//
//public class ShapeLine extends GeoLine {
//
//	public ShapeLine(String id){
//		super(id);
//	}
//	
//	@Override
//	public String toString() {
//		return "ShapeLine [id=" + getId() + " shapeLine=" + getGeoLine().toString() + "]";
//	}
//
//	public String getRouteDistance() {
//		return ((ShapePoint)super.getGeoLine().get(-1)).getDistanceTraveled();
//	}
//	
//}
