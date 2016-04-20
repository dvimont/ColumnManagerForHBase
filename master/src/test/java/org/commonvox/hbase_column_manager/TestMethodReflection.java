/*
 * Copyright (C) 2016 Daniel Vimont
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.commonvox.hbase_column_manager;

import org.commonvox.hbase_column_manager.ChangeEvent;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 *
 * @author dv
 */
public class TestMethodReflection {
    public static void main(String[] args) throws Exception {
        for (Method method : ChangeEvent.class.getDeclaredMethods()) {
            int modifiers = method.getModifiers();
            if (Modifier.isStatic(modifiers) || Modifier.isPublic(modifiers) || Modifier.isPrivate(modifiers)) {
                continue;
            }
            System.out.println("Method: " + method.getName());
            System.out.println(" == method is not static and neither public nor private!!");
            if (method.getParameterCount() == 0) {
                System.out.println(" == method parm count is ZERO");
                if (Comparable.class.isAssignableFrom(method.getReturnType())) {
                    System.out.println(" == method should be added to list of methods");
                }
            }
        }
    }

//    for (Method method : ce) {
//
//    }
    /*
        for (Method method : MASTER_CLASS.getDeclaredMethods()) {
            int modifiers = method.getModifiers();
            if (Modifier.isPublic(modifiers) || Modifier.isPrivate(modifiers)) {
                continue;
            }
            if (method.getParameterCount() == 0) {
                if (Comparable.class.isAssignableFrom(method.getReturnType())) {
                    methodsThatGetComparableObject
                            .put(method.getReturnType().getName(), method);
                } else if (Collection.class.isAssignableFrom(method.getReturnType())) {
                    ParameterizedType listType
                            = (ParameterizedType)method.getGenericReturnType();
                    Type typeOfObjectsInList
                            = listType.getActualTypeArguments()[0];
                    methodsThatGetCollection.put
                                    (typeOfObjectsInList.getTypeName(), method);
                }
            }
        }

    */
}
