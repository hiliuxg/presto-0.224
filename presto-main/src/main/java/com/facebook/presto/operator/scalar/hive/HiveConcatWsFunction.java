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
package com.facebook.presto.operator.scalar.hive;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.bytecode.*;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.IntStream;
import static com.facebook.presto.bytecode.Access.*;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;

public final class HiveConcatWsFunction extends SqlScalarFunction
{
    public static final HiveConcatWsFunction CONCAT_WS =
            new HiveConcatWsFunction(createUnboundedVarcharType().getTypeSignature(), "concatenates given strings");

    private final String description;

    private HiveConcatWsFunction(TypeSignature type, String description)
    {
        super(new Signature(
                "concat_ws",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                type,
                ImmutableList.of(type),
                true));
        this.description = description;
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
        return description;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments");
        }

        Class<?> clazz = generateConcatWs(getSignature().getReturnType(), arity);
        MethodHandle methodHandle = methodHandle(clazz, "concatWs", nCopies(arity, Slice.class).toArray(new Class<?>[arity]));

        return new ScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    private static Class<?> generateConcatWs(TypeSignature type, int arity)
    {
        checkCondition(arity <= 254, NOT_SUPPORTED, "Too many arguments for string concatenation");
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(type.getBase() + "_concat_ws" + arity + "ScalarFunction"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate concat()
        List<Parameter> parameters = IntStream.range(0, arity)
                .mapToObj(i -> arg("arg" + i, Slice.class))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "concatWs", type(Slice.class), parameters);
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable result = scope.declareVariable(Slice.class, "result");
        body.append(result.set(parameters.get(1)));

        Parameter split = parameters.get(0) ;

        for (int i = 2; i < arity; ++i) {
            BytecodeExpression be = invokeStatic(HiveConcatWsFunction.class, "concat", Slice.class,
                    split,parameters.get(i),result);
            body.append(result.set(be));
        }

        body.getVariable(result).retObject();

        return defineClass(definition, Object.class, ImmutableMap.of(), new DynamicClassLoader(HiveConcatWsFunction.class.getClassLoader()));
    }


    @UsedByGeneratedCode
    public static Slice concat(Slice split, Slice param,Slice result)
    {
        try {
            String concat = new StringBuffer().append(result.toStringUtf8()).
                    append(split.toStringUtf8())
                    .append(param.toStringUtf8()).toString();
            return Slices.utf8Slice(concat) ;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
        }
    }
}
