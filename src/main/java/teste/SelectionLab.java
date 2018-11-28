package teste;

import java.awt.Color;

import eu.jacquet80.minigeo.MapWindow;
import eu.jacquet80.minigeo.Point;
import eu.jacquet80.minigeo.Segment;

/**
 * In this example we create a map tool to select a feature clicked
 * with the mouse. The selected feature will be painted yellow.
 *
 * @source $URL: http://svn.osgeo.org/geotools/trunk/demo/example/src/main/java/org/geotools/demo/SelectionLab.java $
 */
public class SelectionLab {

    public static void main(String[] args) {
    	MapWindow window = new MapWindow();
    	window.addSegment( new Segment( new Point(48, 2), new Point(45, -1), Color.RED));
    	window.setVisible(true);
	}
}