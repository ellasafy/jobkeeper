package com.cstor.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializableUtil {
	
	public static final byte[] writeObject(Object object){
		if(object == null){
			return null;
		}
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		byte[] data = null;
		try {
			ObjectOutputStream writer = new ObjectOutputStream(outputStream);
			writer.writeObject(object);
			data = outputStream.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}
	
	public static final Object readObject(byte[] data){
		if(data == null){
			return null;
		}
		ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
		Object object = null;
		try {
			ObjectInputStream reader = new ObjectInputStream(inputStream);
			object = reader.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return object;
	}
}
