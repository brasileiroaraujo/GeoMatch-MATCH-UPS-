/*
 * GenericObject - The Duplicate Detection Toolkit
 * 
 * Copyright (C) 2010  Hasso-Plattner-Institut für Softwaresystemtechnik GmbH,
 *                     Potsdam, Germany 
 *
 * This file is part of GenericObject.
 * 
 * GenericObject is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GenericObject is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with GenericObject.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */

package genericEntity.datasource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import genericEntity.util.data.GenericObject;
import genericEntity.util.data.storage.GenericObjectStorage;
import genericEntity.util.data.storage.InMemoryStorage;
import genericEntity.util.data.storage.JsonableReader;

/**
 * <code>GenericObjectSource</code> is an in-memory {@link DataSource}. All the data is already within the memory. This class can be used to generate
 * data for <code>Generic</code> in a fast and easy way.
 * 
 * @author Matthias Pohl
 */
public class GenericObjectSource extends AbstractDataSource<GenericObjectSource> {

	private final GenericObjectStorage<GenericObject> data;

	private int size = 0;

	/**
	 * Initializes the <code>GenericObjectSource</code> with the passed identifier and {@link GenericObjectStorage} instance.
	 * 
	 * @param id
	 *            The identifier of this {@link DataSource}. All {@link GenericObject}s included in this <code>DataSource</code> have to have this
	 *            identifier as their source id.
	 * @param storage
	 *            The <code>GenericObjectStorage</code> containing the actual data.
	 */
	public GenericObjectSource(String id, GenericObjectStorage<GenericObject> storage) {
		super(id);

		try {
			this.data = storage == null ? new InMemoryStorage<GenericObject>() : storage;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Initializes the <code>GenericObjectSource</code> with the passed identifier and collection.
	 * 
	 * @param id
	 *            The identifier of the {@link DataSource}. All {@link GenericObject}s contained in this <code>DataSource</code> must have this id as
	 *            source identifier.
	 * @param d
	 *            The internally used data.
	 */
	public GenericObjectSource(String id, Collection<GenericObject> d) {
		super(id);

		try {
			this.data = new InMemoryStorage<GenericObject>(d == null ? new ArrayList<GenericObject>() : d);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Iterator<GenericObject> iterator() {
		// the extraction is done after calling iterator() the first time -> hence, the data size is not determined until this method is called
		this.size = this.data.size();

		JsonableReader<GenericObject> reader = this.data.getReader();
		this.registerCloseable(reader);

		return reader.iterator();
	}

	@Override
	public void close() throws IOException {
		// nothing to do
	}

	@Override
	public void addIdAttributes(String... idAttributes) {
		throw new UnsupportedOperationException("The id's are already defined within the GenericObjects.");
	}

	@Override
	protected boolean autoGeneratedIds() {
		throw new UnsupportedOperationException("The id's are already defined within the GenericObjects.");
	}

	@Override
	protected Iterable<String> getIdAttributes() {
		throw new UnsupportedOperationException("The id's are already defined within the GenericObjects.");
	}

	@Override
	public int getExtractedRecordCount() {
		// the size is determined while calling iterator() the first time -> returning this.data.size() instead of this.size would be invalid
		return this.size;
	}

}