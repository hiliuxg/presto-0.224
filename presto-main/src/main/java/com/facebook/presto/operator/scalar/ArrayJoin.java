/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public final class ArrayJoin
        extends SqlScalarFunction
{
    public static final ArrayJoin ARRAY_JOIN = new ArrayJoin();
    public static final ArrayJoinWithNullReplacement ARRAY_JOIN_WITH_NULL_REPLACEMENT = new ArrayJoinWithNullReplacement();

    private static final TypeSignature VARCHAR_TYPE_SIGNATURE = VARCHAR.getTypeSignature();
    private static final String FUNCTION_NAME = "array_join";
    private static final String DESCRIPTION = "Concatenates the elements of the given array using a delimiter and an optional string to replace nulls";
    private static final MethodHandle METHOD_HANDLE = methodHandle(
            ArrayJoin.class,
            "arrayJoin",
            MethodHandle.class,
            Object.class,
            ConnectorSession.class,
            Block.class,
            Slice.class);

    private static final MethodHandle GET_BOOLEAN = methodHandle(Type.class, "getBoolean", Block.class, int.class);
    private static final MethodHandle GET_DOUBLE = methodHandle(Type.class, "getDouble", Block.class, int.class);
    private static final MethodHandle GET_LONG = methodHandle(Type.class, "getLong", Block.class, int.class);
    private static final MethodHandle GET_SLICE = methodHandle(Type.class, "getSlice", Block.class, int.class);

    private static final MethodHandle STATE_FACTORY = methodHandle(ArrayJoin.class, "createState");

    public static class ArrayJoinWithNullReplacement
            extends SqlScalarFunction
    {
        private static final MethodHandle METHOD_HANDLE = methodHandle(
                ArrayJoin.class,
                "arrayJoin",
                MethodHandle.class,
                Object.class,
                ConnectorSession.class,
                Block.class,
                Slice.class,
                Slice.class);

        public ArrayJoinWithNullReplacement()
        {
            super(new Signature(FUNCTION_NAME,
                    FunctionKind.SCALAR,
                    ImmutableList.of(typeVariable("T")),
                    ImmutableList.of(),
                    parseTypeSignature(StandardTypes.VARCHAR),
                    ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)),
                    false));
        }

        @Override
        public boolean isHidden()
        {
            return false;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public String getDescription()
        {
            return DESCRIPTION;
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
        {
            return specializeArrayJoin(boundVariables.getTypeVariables(), functionManager, ImmutableList.of(false, false, false), METHOD_HANDLE);
        }
    }

    public ArrayJoin()
    {
        super(new Signature(FUNCTION_NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.VARCHAR),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature(StandardTypes.VARCHAR)),
                false));
    }

    @UsedByGeneratedCode
    public static Object createState()
    {
        return new PageBuilder(ImmutableList.of(VARCHAR));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return DESCRIPTION;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        return specializeArrayJoin(boundVariables.getTypeVariables(), functionManager, ImmutableList.of(false, false), METHOD_HANDLE);
    }

    private static ScalarFunctionImplementation specializeArrayJoin(Map<String, Type> types, FunctionManager functionManager, List<Boolean> nullableArguments, MethodHandle methodHandle)
    {
        Type type = types.get("T");
        List<ArgumentProperty> argumentProperties = nullableArguments.stream()
                .map(nullable -> nullable
                        ? valueTypeArgumentProperty(USE_BOXED_TYPE)
                        : valueTypeArgumentProperty(RETURN_NULL_ON_NULL))
                .collect(toImmutableList());

        if (type instanceof UnknownType) {
            return new ScalarFunctionImplementation(
                    false,
                    argumentProperties,
                    methodHandle.bindTo(null),
                    Optional.of(STATE_FACTORY));
        }
        else {
            try {
                ScalarFunctionImplementation castFunction = functionManager.getScalarFunctionImplementation(functionManager.lookupCast(CAST, type.getTypeSignature(), VARCHAR_TYPE_SIGNATURE));

                MethodHandle getter;
                Class<?> elementType = type.getJavaType();
                if (elementType == boolean.class) {
                    getter = GET_BOOLEAN;
                }
                else if (elementType == double.class) {
                    getter = GET_DOUBLE;
                }
                else if (elementType == long.class) {
                    getter = GET_LONG;
                }
                else if (elementType == Slice.class) {
                    getter = GET_SLICE;
                }
                else {
                    throw new UnsupportedOperationException("Unsupported type: " + elementType.getName());
                }

                MethodHandle cast = castFunction.getMethodHandle();

                // if the cast doesn't take a ConnectorSession, create an adapter that drops the provided session
                if (cast.type().parameterArray()[0] != ConnectorSession.class) {
                    cast = MethodHandles.dropArguments(cast, 0, ConnectorSession.class);
                }

                // Adapt a target cast that takes (ConnectorSession, ?) to one that takes (Block, int, ConnectorSession), which will be invoked by the implementation
                // The first two arguments (Block, int) are filtered through the element type's getXXX method to produce the underlying value that needs to be passed to
                // the cast.
                cast = MethodHandles.permuteArguments(cast, MethodType.methodType(Slice.class, cast.type().parameterArray()[1], cast.type().parameterArray()[0]), 1, 0);
                cast = MethodHandles.dropArguments(cast, 1, int.class);
                cast = MethodHandles.dropArguments(cast, 1, Block.class);
                cast = MethodHandles.foldArguments(cast, getter.bindTo(type));

                MethodHandle target = MethodHandles.insertArguments(methodHandle, 0, cast);
                return new ScalarFunctionImplementation(
                        false,
                        argumentProperties,
                        target,
                        Optional.of(STATE_FACTORY));
            }
            catch (PrestoException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Input type %s not supported", type), e);
            }
        }
    }

    @UsedByGeneratedCode
    public static Slice arrayJoin(
            MethodHandle castFunction,
            Object state,
            ConnectorSession session,
            Block arrayBlock,
            Slice delimiter)
    {
        return arrayJoin(castFunction, state, session, arrayBlock, delimiter, null);
    }

    @UsedByGeneratedCode
    public static Slice arrayJoin(
            MethodHandle castFunction,
            Object state,
            ConnectorSession session,
            Block arrayBlock,
            Slice delimiter,
            Slice nullReplacement)
    {
        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        int numElements = arrayBlock.getPositionCount();
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);

        for (int i = 0; i < numElements; i++) {
            if (arrayBlock.isNull(i)) {
                if (nullReplacement != null) {
                    blockBuilder.writeBytes(nullReplacement, 0, nullReplacement.length());
                }
                else {
                    continue;
                }
            }
            else {
                try {
                    Slice slice = (Slice) castFunction.invokeExact(arrayBlock, i, session);
                    blockBuilder.writeBytes(slice, 0, slice.length());
                }
                catch (Throwable throwable) {
                    // Restore pageBuilder into a consistent state
                    blockBuilder.closeEntry();
                    pageBuilder.declarePosition();
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error casting array element to VARCHAR", throwable);
                }
            }

            if (i != numElements - 1) {
                blockBuilder.writeBytes(delimiter, 0, delimiter.length());
            }
        }

        blockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return VARCHAR.getSlice(blockBuilder, blockBuilder.getPositionCount() - 1);
    }
}
