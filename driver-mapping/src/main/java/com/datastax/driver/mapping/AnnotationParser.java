/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.mapping.MethodMapper.EnumParamMapper;
import com.datastax.driver.mapping.MethodMapper.NestedUDTParamMapper;
import com.datastax.driver.mapping.MethodMapper.ParamMapper;
import com.datastax.driver.mapping.MethodMapper.UDTParamMapper;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.base.Strings;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Static metods that facilitates parsing class annotations into the corresponding {@link EntityMapper}.
 */
class AnnotationParser {

    private static final Comparator<Field> fieldComparator = new Comparator<Field>() {
        @Override
        public int compare(Field f1, Field f2) {
            return position(f1) - position(f2);
        }
    };

    private AnnotationParser() {
    }

    public static <T> EntityMapper<T> parseEntity(Class<T> entityClass, EntityMapper.Factory factory, MappingManager mappingManager) {
        Table table = AnnotationChecks.getTypeAnnotation(Table.class, entityClass);

        String ksName = table.caseSensitiveKeyspace() ? table.keyspace() : table.keyspace().toLowerCase();
        String tableName = table.caseSensitiveTable() ? table.name() : table.name().toLowerCase();

        ConsistencyLevel writeConsistency = table.writeConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.writeConsistency().toUpperCase());
        ConsistencyLevel readConsistency = table.readConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.readConsistency().toUpperCase());

        if (Strings.isNullOrEmpty(table.keyspace())) {
            ksName = mappingManager.getSession().getLoggedKeyspace();
            if (Strings.isNullOrEmpty(ksName))
                throw new IllegalArgumentException(String.format(
                        "Error creating mapper for class %s, the @%s annotation declares no default keyspace, and the session is not currently logged to any keyspace",
                        entityClass.getSimpleName(),
                        Table.class.getSimpleName()
                ));
        }

        EntityMapper<T> mapper = factory.create(entityClass, ksName, tableName, writeConsistency, readConsistency);

        List<Field> pks = new ArrayList<Field>();
        List<Field> ccs = new ArrayList<Field>();
        List<Field> rgs = new ArrayList<Field>();

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isSynthetic() || (field.getModifiers() & Modifier.STATIC) == Modifier.STATIC)
                continue;

            if (mappingManager.isCassandraV1 && field.getAnnotation(Computed.class) != null)
                throw new UnsupportedOperationException("Computed fields are not supported with native protocol v1");

            AnnotationChecks.validateAnnotations(field, "entity",
                    Column.class, ClusteringColumn.class, Enumerated.class, Frozen.class, FrozenKey.class,
                    FrozenValue.class, PartitionKey.class, Transient.class, Computed.class);

            if (field.getAnnotation(Transient.class) != null)
                continue;

            switch (kind(field)) {
                case PARTITION_KEY:
                    pks.add(field);
                    break;
                case CLUSTERING_COLUMN:
                    ccs.add(field);
                    break;
                default:
                    rgs.add(field);
                    break;
            }
        }

        AtomicInteger columnCounter = mappingManager.isCassandraV1 ? null : new AtomicInteger(0);

        Collections.sort(pks, fieldComparator);
        Collections.sort(ccs, fieldComparator);

        validateOrder(pks, "@PartitionKey");
        validateOrder(ccs, "@ClusteringColumn");

        mapper.addColumns(convert(pks, factory, mapper.entityClass, mappingManager, columnCounter),
                convert(ccs, factory, mapper.entityClass, mappingManager, columnCounter),
                convert(rgs, factory, mapper.entityClass, mappingManager, columnCounter));
        return mapper;
    }

    public static <T> EntityMapper<T> parseUDT(Class<T> udtClass, EntityMapper.Factory factory, MappingManager mappingManager) {
        UDT udt = AnnotationChecks.getTypeAnnotation(UDT.class, udtClass);

        String ksName = udt.caseSensitiveKeyspace() ? udt.keyspace() : udt.keyspace().toLowerCase();
        String udtName = udt.caseSensitiveType() ? udt.name() : udt.name().toLowerCase();

        if (Strings.isNullOrEmpty(udt.keyspace())) {
            ksName = mappingManager.getSession().getLoggedKeyspace();
            if (Strings.isNullOrEmpty(ksName))
                throw new IllegalArgumentException(String.format(
                        "Error creating UDT mapper for class %s, the @%s annotation declares no default keyspace, and the session is not currently logged to any keyspace",
                        udtClass.getSimpleName(),
                        UDT.class.getSimpleName()
                ));
        }

        EntityMapper<T> mapper = factory.create(udtClass, ksName, udtName, null, null);

        List<Field> columns = new ArrayList<Field>();

        for (Field field : udtClass.getDeclaredFields()) {
            if (field.isSynthetic() || (field.getModifiers() & Modifier.STATIC) == Modifier.STATIC)
                continue;

            AnnotationChecks.validateAnnotations(field, "UDT",
                    com.datastax.driver.mapping.annotations.Field.class, Frozen.class, FrozenKey.class,
                    FrozenValue.class, Enumerated.class, Transient.class);

            if (field.getAnnotation(Transient.class) != null)
                continue;

            switch (kind(field)) {
                case PARTITION_KEY:
                    throw new IllegalArgumentException("Annotation @PartitionKey is not allowed in a class annotated by @UDT");
                case CLUSTERING_COLUMN:
                    throw new IllegalArgumentException("Annotation @ClusteringColumn is not allowed in a class annotated by @UDT");
                default:
                    columns.add(field);
                    break;
            }
        }

        mapper.addColumns(convert(columns, factory, udtClass, mappingManager, null));
        return mapper;
    }

    private static <T> List<ColumnMapper<T>> convert(List<Field> fields, EntityMapper.Factory factory, Class<T> klass, MappingManager mappingManager, AtomicInteger columnCounter) {
        List<ColumnMapper<T>> mappers = new ArrayList<ColumnMapper<T>>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            int pos = position(field);
            mappers.add(factory.createColumnMapper(klass, field, pos < 0 ? i : pos, mappingManager, columnCounter));
        }
        return mappers;
    }

    private static void validateOrder(List<Field> fields, String annotation) {
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            int pos = position(field);
            if (pos != i)
                throw new IllegalArgumentException(String.format("Invalid ordering value %d for annotation %s of column %s, was expecting %d",
                        pos, annotation, field.getName(), i));
        }
    }

    private static int position(Field field) {
        switch (kind(field)) {
            case PARTITION_KEY:
                return field.getAnnotation(PartitionKey.class).value();
            case CLUSTERING_COLUMN:
                return field.getAnnotation(ClusteringColumn.class).value();
            default:
                return -1;
        }
    }

    public static ColumnMapper.Kind kind(Field field) {
        PartitionKey pk = field.getAnnotation(PartitionKey.class);
        ClusteringColumn cc = field.getAnnotation(ClusteringColumn.class);
        Computed comp = field.getAnnotation(Computed.class);
        if (pk != null && cc != null)
            throw new IllegalArgumentException("Field " + field.getName() + " cannot have both the @PartitionKey and @ClusteringColumn annotations");

        if (pk != null) {
            return ColumnMapper.Kind.PARTITION_KEY;
        }
        if (cc != null) {
            return ColumnMapper.Kind.CLUSTERING_COLUMN;
        }
        if (comp != null) {
            return ColumnMapper.Kind.COMPUTED;
        }
        return ColumnMapper.Kind.REGULAR;
    }

    public static EnumType enumType(Field field) {
        Class<?> type = field.getType();
        if (!type.isEnum())
            return null;

        Enumerated enumerated = field.getAnnotation(Enumerated.class);
        return (enumerated == null) ? EnumType.STRING : enumerated.value();
    }

    public static String columnName(Field field) {
        Column column = field.getAnnotation(Column.class);
        Computed computedField = field.getAnnotation(Computed.class);
        if (column != null && !column.name().isEmpty()) {
            if (computedField != null) {
                throw new IllegalArgumentException("Cannot use @Column and @Computed on the same field");
            }
            return column.caseSensitive() ? column.name() : column.name().toLowerCase();
        }

        com.datastax.driver.mapping.annotations.Field udtField = field.getAnnotation(com.datastax.driver.mapping.annotations.Field.class);
        if (udtField != null && !udtField.name().isEmpty()) {
            return udtField.caseSensitive() ? udtField.name() : udtField.name().toLowerCase();
        }

        if (computedField != null) {
            return computedField.value();
        }

        return field.getName().toLowerCase();
    }

    public static String newAlias(Field field, int columnNumber) {
        return "col" + columnNumber;

    }

    public static <T> AccessorMapper<T> parseAccessor(Class<T> accClass, AccessorMapper.Factory factory, MappingManager mappingManager) {
        if (!accClass.isInterface())
            throw new IllegalArgumentException("@Accessor annotation is only allowed on interfaces");

        AnnotationChecks.getTypeAnnotation(Accessor.class, accClass);

        List<MethodMapper> methods = new ArrayList<MethodMapper>();
        for (Method m : accClass.getDeclaredMethods()) {
            Query query = m.getAnnotation(Query.class);
            if (query == null)
                continue;

            String queryString = query.value();

            Annotation[][] paramAnnotations = m.getParameterAnnotations();
            Type[] paramTypes = m.getGenericParameterTypes();
            ParamMapper[] paramMappers = new ParamMapper[paramAnnotations.length];
            Boolean hasParamAnnotation = null;
            for (int i = 0; i < paramMappers.length; i++) {
                String paramName = null;
                for (Annotation a : paramAnnotations[i]) {
                    if (a.annotationType().equals(Param.class)) {
                        paramName = ((Param) a).value();
                        break;
                    }
                }
                if (hasParamAnnotation == null)
                    hasParamAnnotation = (paramName != null);
                if (hasParamAnnotation != (paramName != null))
                    throw new IllegalArgumentException(String.format("For method '%s', either all or none of the paramaters of a method must have a @Param annotation", m.getName()));

                paramMappers[i] = newParamMapper(accClass.getName(), m.getName(), i, paramName, paramTypes[i], paramAnnotations[i], mappingManager);
            }

            ConsistencyLevel cl = null;
            int fetchSize = -1;
            boolean tracing = false;

            QueryParameters options = m.getAnnotation(QueryParameters.class);
            if (options != null) {
                cl = options.consistency().isEmpty() ? null : ConsistencyLevel.valueOf(options.consistency().toUpperCase());
                fetchSize = options.fetchSize();
                tracing = options.tracing();
            }

            methods.add(new MethodMapper(m, queryString, paramMappers, cl, fetchSize, tracing));
        }

        return factory.create(accClass, methods);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ParamMapper newParamMapper(String className, String methodName, int idx, String paramName, Type paramType, Annotation[] paramAnnotations, MappingManager mappingManager) {
        if (paramType instanceof Class) {
            Class<?> paramClass = (Class<?>) paramType;
            if (paramClass.isAnnotationPresent(UDT.class)) {
                UDTMapper<?> udtMapper = mappingManager.getUDTMapper(paramClass);
                return new UDTParamMapper(paramName, idx, udtMapper);
            } else if (paramClass.isEnum()) {
                EnumType enumType = EnumType.STRING;
                for (Annotation annotation : paramAnnotations) {
                    if (annotation instanceof Enumerated) {
                        enumType = ((Enumerated) annotation).value();
                    }
                }
                return new EnumParamMapper(paramName, idx, enumType);
            }
            return new ParamMapper(paramName, idx);
        }
        if (paramType instanceof ParameterizedType) {
            InferredCQLType inferredCQLType = InferredCQLType.from(className, methodName, idx, paramName, paramType, mappingManager);
            if (inferredCQLType.containsMappedUDT) {
                // We need a specialized mapper to convert UDT instances in the hierarchy.
                return new NestedUDTParamMapper(paramName, idx, inferredCQLType);
            } else {
                // Use the default mapper but provide the extracted type
                return new ParamMapper(paramName, idx, inferredCQLType.dataType);
            }
        } else {
            throw new IllegalArgumentException(String.format("Cannot map class %s for parameter %s of %s.%s", paramType, paramName, className, methodName));
        }
    }
}
