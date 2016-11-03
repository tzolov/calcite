package org.apache.calcite.adapter.geode.simple;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.geode.util.JavaTypeFactoryExtImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import net.tzolov.geode.bookstore.domain.Address;
import net.tzolov.geode.bookstore.domain.BookMaster;
import net.tzolov.geode.bookstore.domain.Customer;


/**
 * Created by tzoloc on 5/4/16.
 */
public class Main2Foo {

	public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
		testCustomer();
		//ReflectiveSchema
	}

	public static void testBookMaster() {
		RelDataType relDataType = new JavaTypeFactoryExtImpl().createStructType(BookMaster.class);

		System.out.println(relDataType.getSqlTypeName());

		BookMaster bookMaster = new BookMaster(666, "boza", 666.666f, 89, "author", "title");

		List<RelDataTypeField> fieldList = relDataType.getFieldList();

		for (Object o : getValue(relDataType, bookMaster)) {
			System.out.println(o);
		}
	}

	public static void testCustomer() {

		ArrayList<Integer> list = new ArrayList<Integer>();
		list.add(666);
		int index = 0;
		Address address = new Address("addressLine1" + index, "addressLine2" + index,
				"addressLine3" + index, "city" + index, "state" + index, "postalCode" + index,
				"country" + index, "phoneNumber" + index, "addressTag" + index);

		Customer customer = new Customer(new Integer(666), "Foo", "Boo", address, list);

		RelDataType relDataType = new JavaTypeFactoryExtImpl().createStructType(Customer.class);

		for (Object o : getValue(relDataType, customer)) {
			System.out.println(o);
		}

	}

	public static Object[] getValue(RelDataType relDataType, Object obj) {

		List<RelDataTypeField> relDataTypeFields = relDataType.getFieldList();

		Object[] values = new Object[relDataTypeFields.size()];

		Class<?> clazz = obj.getClass();

		int index = 0;
		for (RelDataTypeField relDataTypeField : relDataTypeFields) {
			try {
				Field javaField = clazz.getDeclaredField(relDataTypeField.getName());
				javaField.setAccessible(true);
				values[index++] = javaField.get(obj);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		return values;
	}
}
