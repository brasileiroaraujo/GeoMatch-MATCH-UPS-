package LineDependencies;

import java.io.Serializable;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import PolygonDependencies.InputTypes;

public class GeoLine extends GeoObject implements Serializable{
	
	//Used in polygons matching
	public GeoLine(String geometryGIS, String geoName, InputTypes type, Integer idGeometry, Integer idInDataset) throws ParseException {
		super(geometryGIS, type, idGeometry, geoName, idInDataset);
	}
	
	@Override
	public Geometry getGeometry() {
		return geometry;
	}
	@Override
	public void setGeometry(Geometry geometry) {
		this.geometry = geometry;
	}

	@Override
	public Integer getIdGeometry() {
		return idGeometry;
	}

	@Override
	public void setIdGeometry(Integer idGeometry) {
		this.idGeometry = idGeometry;
	}

	@Override
	public String getGeoName() {
		return geoName;
	}

	@Override
	public void setGeoName(String geoName) {
		this.geoName = geoName;
	}
	@Override
	public InputTypes getType() {
		return type;
	}
	@Override
	public void setType(InputTypes type) {
		this.type = type;
	}

	@Override
	public Integer getIdInDataset() {
		return idInDataset;
	}

	@Override
	public void setIdInDataset(Integer idInDataset) {
		this.idInDataset = idInDataset;
	}
	

}
