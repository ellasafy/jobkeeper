package com.cstor.jobkeeper.Serializable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Serializable {
	/**
	 * serialize object to byte[] that zookeeper can use
	 * @param object Object
	 * @return byte[]
	 * @throws IOException
	 */
	public byte[] serialize(Object object) throws IOException {
		if (object == null) {
			return null;
		}
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		byte[] data = null;
		ObjectOutputStream writer = new ObjectOutputStream(outputStream);
		writer.writeObject(object);
		data = outputStream.toByteArray();
		writer.close();
		return data;

	}
	
	/**
	 * deserialize from byte[] to object
	 * @param data byte[]
	 * @return Object
	 * @throws ClassNotFoundException 
	 * @throws IOException
	 */
	public Object deserialize(byte[] data) throws ClassNotFoundException,
			IOException {
		if (data == null) {
			return null;
		}
		ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
		Object object = null;
		ObjectInputStream reader = new ObjectInputStream(inputStream);
		object = reader.readObject();
		return object;
	}

}
