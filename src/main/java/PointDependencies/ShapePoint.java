package PointDependencies;

import java.util.StringTokenizer;

public class ShapePoint extends GeoPoint{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//"shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled"
	//shape: 3217,-25.4757686477818,-49.2923877163312,3281146,24.441
	private String id;
	private String pointSequence;
	private String distanceTraveled;
	
	public ShapePoint(String id, String latitude, String longitude, String pointSequence, String distanceTraveled) {
		super(latitude, longitude);
		this.id = id;
		this.pointSequence = pointSequence;
		this.distanceTraveled = distanceTraveled;
	}
	
	public ShapePoint() {
		super("", "");
		this.id = "";
		this.pointSequence = "";
		this.distanceTraveled = "";
	}

	public static ShapePoint createShapePoint(String line) {
		StringTokenizer st = new StringTokenizer(line, ",");
		return new ShapePoint(st.nextToken().replace("\"", ""),
				st.nextToken().replace("\"", ""),
				st.nextToken().replace("\"", ""),
				st.nextToken().replace("\"", ""),
				st.nextToken().replace("\"", ""));
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPointSequence() {
		return pointSequence;
	}

	public void setPointSequence(String pointSequence) {
		this.pointSequence = pointSequence;
	}

	public String getDistanceTraveled() {
		return distanceTraveled;
	}

	public void setDistanceTraveled(String distanceTraveled) {
		this.distanceTraveled = distanceTraveled;
	}

	@Override
	public String toString() {
		return "ShapePoint [id=" + id + ", pointSequence=" + pointSequence + ", distanceTraveled=" + distanceTraveled
				+ super.toString() +"]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((distanceTraveled == null) ? 0 : distanceTraveled.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((pointSequence == null) ? 0 : pointSequence.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ShapePoint other = (ShapePoint) obj;
		if (distanceTraveled == null) {
			if (other.distanceTraveled != null)
				return false;
		} else if (!distanceTraveled.equals(other.distanceTraveled))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (pointSequence == null) {
			if (other.pointSequence != null)
				return false;
		} else if (!pointSequence.equals(other.pointSequence))
			return false;
		return true;
	}
	
}
