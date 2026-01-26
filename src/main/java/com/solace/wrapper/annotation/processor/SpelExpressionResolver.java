package com.solace.wrapper.annotation.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.StandardReflectionParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * Comprehensive SpEL expression resolver that handles all possible scenarios.
 * This utility class provides consistent SpEL expression processing across
 * the entire Solace wrapper framework.
 *
 * Supports:
 * - Variable references: #variableName, #param0, #result
 * - Bean references: @beanName, @beanName.property
 * - Template expressions: #{expression}
 * - Type references: T(java.lang.System)
 */
@Component
public class SpelExpressionResolver {

    private static final Logger logger = LoggerFactory.getLogger(SpelExpressionResolver.class);

    private final ExpressionParser expressionParser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNameDiscoverer = new StandardReflectionParameterNameDiscoverer();

    private BeanFactory beanFactory;

    /**
     * Default constructor for non-Spring usage (limited functionality - no bean references).
     */
    public SpelExpressionResolver() {
        // BeanFactory will be null - @beanName references won't work
    }

    /**
     * Setter for BeanFactory injection to enable @beanName references in SpEL.
     */
    @Autowired(required = false)
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
        logger.debug("BeanFactory injected - @beanName references are now supported");
    }
    
    // Patterns that could indicate SpEL injection attempts
    private static final String[] DANGEROUS_PATTERNS = {
        "T(java.lang.Runtime)",
        "T(Runtime)",
        "getRuntime()",
        "exec(",
        "ProcessBuilder",
        "getClass().forName",
        "Class.forName",
        "getClass().getClassLoader",
        "ClassLoader",
        "java.io.File",
        "java.nio.file",
        "FileOutputStream",
        "FileInputStream",
        "FileWriter",
        "FileReader",
        "java.net.URL",
        "java.net.Socket",
        "javax.script",
        "ScriptEngine",
        "getenv(",
        "System.getProperty",
        "System.setProperty",
        "System.exit",
        "Thread.sleep",
        "Runtime.getRuntime",
        "ProcessBuilder",
        ".invoke(",
        "getDeclaredMethod",
        "getDeclaredField",
        "setAccessible"
    };

    /**
     * Validates that a SpEL expression does not contain potentially dangerous patterns.
     * This is a defense-in-depth measure against SpEL injection attacks.
     *
     * @param expression The expression to validate
     * @throws IllegalArgumentException if the expression contains dangerous patterns
     */
    public void validateExpression(String expression) {
        if (expression == null || expression.isEmpty()) {
            return;
        }

        String lowerExpression = expression.toLowerCase();
        for (String pattern : DANGEROUS_PATTERNS) {
            if (lowerExpression.contains(pattern.toLowerCase())) {
                logger.warn("Potentially unsafe SpEL expression detected: '{}' contains pattern '{}'", expression, pattern);
                throw new IllegalArgumentException(
                        "SpEL expression contains potentially unsafe pattern: " + pattern +
                        ". If this is intentional, please review the security implications.");
            }
        }
    }

    /**
     * Resolves a SpEL expression with the given context and expected type.
     * This is the main entry point for all SpEL expression resolution.
     *
     * @param expression The expression to resolve
     * @param context The evaluation context
     * @param expectedType The expected return type
     * @return The resolved value cast to the expected type
     */
    @SuppressWarnings("null")
    public <T> T resolveExpression(String expression, EvaluationContext context, Class<T> expectedType) {
        if (expression == null) {
            return null;
        }

        // Only treat as empty if it's truly empty (no characters at all)
        if (expression.isEmpty()) {
            return null;
        }

        // Validate expression for potential injection attacks
        validateExpression(expression);
        
        logger.debug("Resolving expression: '{}' with context: {}", expression, context);
        
        try {
            // Check if it contains template placeholders like #{variableName}
            if (expression.contains("#{")) {
                logger.debug("Detected template expression, resolving placeholders...");
                String resolved = resolveTemplate(expression, context);
                logger.debug("Template resolution result: {}", resolved);
                
                // If the resolved expression still contains variables and we need a boolean result,
                // try to evaluate it as a SpEL expression
                if (expectedType == Boolean.class && resolved.contains(" ")) {
                    logger.debug("Evaluating resolved expression as SpEL: {}", resolved);
                    Expression expr = expressionParser.parseExpression(resolved);
                    return expr.getValue(context, expectedType);
                }
                
                // Handle type conversion properly for resolved template expressions
                return convertToExpectedType(resolved, expectedType);
            } else {
                // Check if this is a direct SpEL expression (starts with #)
                if (expression.startsWith("#")) {
                    logger.debug("Detected direct SpEL expression: {}", expression);
                    try {
                        Expression expr = expressionParser.parseExpression(expression);
                        Object value = expr.getValue(context);
                        if (value != null) {
                            return expectedType.cast(value);
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to evaluate direct SpEL expression '{}', treating as literal", expression, e);
                    }
                } else {
                    // CRITICAL FIX: Always try to evaluate as SpEL expression first
                    // This handles cases like "order.orderId", "order.orderId + '_' + customerId", etc.
                    logger.debug("Attempting to evaluate as SpEL expression: {}", expression);
                    try {
                        // Add variable prefixes to the expression before parsing
                        String processedExpression = addVariablePrefixes(expression);
                        logger.debug("Processed expression: {}", processedExpression);
                        
                        Expression expr = expressionParser.parseExpression(processedExpression);
                        Object value = expr.getValue(context);
                        if (value != null) {
                            logger.debug("Successfully evaluated SpEL expression '{}' to: {}", expression, value);
                            return convertToExpectedType(value, expectedType);
                        }
                    } catch (Exception e) {
                        logger.debug("Expression '{}' is not a valid SpEL expression, treating as literal: {}", expression, e.getMessage());
                    }
                }
                
                logger.debug("Treating as literal value");
                // Handle type conversion for literals
                return convertToExpectedType(expression, expectedType);
            }
        } catch (Exception e) {
            logger.warn("Failed to evaluate expression '{}', using as literal", expression, e);
            // Handle type conversion properly in fallback case
            return convertToExpectedType(expression, expectedType);
        }
    }
    
    /**
     * Creates an evaluation context with parameter discovery for method-based SpEL expressions.
     * 
     * @param method The method to create context for
     * @param args The method arguments
     * @param result The method result (can be null)
     * @return Configured evaluation context
     */
    public EvaluationContext createEvaluationContext(Method method, Object[] args, Object result) {
        return createEvaluationContext(method, args, result, null);
    }
    
    /**
     * Creates an evaluation context with parameter discovery for method-based SpEL expressions.
     *
     * @param method The method to create context for
     * @param args The method arguments
     * @param result The method result (can be null)
     * @param target The target object (for accessing instance fields)
     * @return Configured evaluation context
     */
    @SuppressWarnings("null")
    public EvaluationContext createEvaluationContext(Method method, Object[] args, Object result, Object target) {
        StandardEvaluationContext context = new StandardEvaluationContext();

        // CRITICAL: Add BeanResolver to support @beanName references in SpEL expressions
        if (beanFactory != null) {
            context.setBeanResolver(new BeanFactoryResolver(beanFactory));
            logger.debug("BeanResolver configured for SpEL context - @beanName references enabled");
        } else {
            logger.debug("No BeanFactory available - @beanName references will not work");
        }

        // Set target object as a variable so it can be accessed but doesn't override parameter resolution
        if (target != null) {
            context.setVariable("target", target);
        }

        logger.debug("Method: {}, Arguments: {}",
                    method.getName(),
                    java.util.Arrays.toString(args));

        // Set up SpEL-native context with args array
        context.setVariable("args", args);
        
        // Robust parameter discovery mechanism - tries multiple strategies to find real parameter names
        String[] discoveredNames = discoverParameterNamesRobustly(method);
        
        logger.debug("Method: {}, Discovered parameter names: {}", method.getName(), java.util.Arrays.toString(discoveredNames));
        logger.debug("Method: {}, Arguments: {}", method.getName(), java.util.Arrays.toString(args));
        
        if (discoveredNames != null && discoveredNames.length > 0) {
            // Set the discovered parameter names (only real names)
            for (int i = 0; i < discoveredNames.length && i < args.length; i++) {
                if (discoveredNames[i] != null) {
                    context.setVariable(discoveredNames[i], args[i]);
                    logger.debug("Set discovered parameter name: {} = {}", discoveredNames[i], args[i]);
                }
            }
        } else {
            logger.debug("No real parameter names could be discovered for method: {}", method.getName());
        }
        
        // Set up indexed access for fallback (args[0], args[1], etc.)
        // This provides a reliable way to access parameters when real names can't be discovered
        for (int i = 0; i < args.length; i++) {
            context.setVariable("arg" + i, args[i]);
            logger.debug("Set indexed parameter: arg{} = {}", i, args[i]);
        }
        
        // Add return value (but don't override parameter names)
        if (result != null) {
            context.setVariable("result", result);
        }
        
        return context;
    }

    /**
     * Creates a simple evaluation context that only supports @beanName references.
     * Use this when resolving expressions at startup/configuration time where
     * method arguments are not available.
     *
     * @return Configured evaluation context with BeanResolver
     */
    public EvaluationContext createBeanOnlyContext() {
        StandardEvaluationContext context = new StandardEvaluationContext();

        // CRITICAL: Add BeanResolver to support @beanName references in SpEL expressions
        if (beanFactory != null) {
            context.setBeanResolver(new BeanFactoryResolver(beanFactory));
            logger.debug("BeanResolver configured for bean-only SpEL context");
        } else {
            logger.warn("No BeanFactory available - @beanName references will not work in SpEL expressions");
        }

        return context;
    }

    /**
     * Resolves template expressions by finding and replacing #{...} placeholders.
     */
    private String resolveTemplate(String template, EvaluationContext context) {
        if (template == null || template.isEmpty()) {
            return template;
        }
        
        String result = template;
        
        // Find all #{expression} patterns and replace them
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("#\\{([^}]+)\\}");
        java.util.regex.Matcher matcher = pattern.matcher(template);
        
        while (matcher.find()) {
            String placeholder = matcher.group(0); // e.g., "#{category}" or "#{priority == 'important'}"
            String expression = matcher.group(1); // e.g., "category" or "priority == 'important'"
            
            try {
                // Process the expression to add # prefixes to variables if needed
                String processedExpression = addVariablePrefixes(expression);
                
                logger.debug("Evaluating SpEL expression: {}", processedExpression);
                @SuppressWarnings("null")
                Expression expr = expressionParser.parseExpression(processedExpression);
                @SuppressWarnings("null")
                Object value = expr.getValue(context);
                if (value != null) {
                    String stringValue = value.toString();
                    result = result.replace(placeholder, stringValue);
                    logger.debug("Replaced {} with {}", placeholder, stringValue);
                } else {
                    // If value is null, keep the original placeholder
                    logger.debug("Value is null for expression '{}', keeping original placeholder", expression);
                }
            } catch (Exception e) {
                logger.warn("Failed to evaluate SpEL expression '{}', keeping original placeholder", expression, e);
                // Keep the original placeholder if evaluation fails - don't replace it
                // This ensures that non-existent properties return the original expression
                // The placeholder is already in the result, so we don't need to do anything
            }
        }
        
        return result;
    }
    
    // Variable prefixing and operator conversion methods
    private String addVariablePrefixes(String expression) {
        String result = expression;
        logger.debug("addVariablePrefixes input: {}", expression);

        // CRITICAL: Do NOT modify expressions that start with @ (bean references)
        // Bean references like @beanName or @beanName.property should be left as-is
        if (result.startsWith("@")) {
            logger.debug("Expression starts with @ (bean reference) - no prefixing needed: {}", result);
            return result;
        }

        // Handle between operator first - convert "X between Y and Z" to "X between {Y, Z}"
        result = convertBetweenOperator(result);
        logger.debug("After convertBetweenOperator: {}", result);

        // First, try the simple approach for expressions starting with a single variable
        java.util.regex.Pattern firstWordPattern = java.util.regex.Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)");
        java.util.regex.Matcher firstWordMatcher = firstWordPattern.matcher(result);

        if (firstWordMatcher.find()) {
            String firstWord = firstWordMatcher.group(1);
            // Only prefix if it's not already prefixed and not a SpEL keyword
            if (!isSpelKeyword(firstWord) && !isOperator(firstWord) && !result.startsWith("#")) {
                // Check if this is a simple expression (single variable or property chain)
                if (result.matches("^[a-zA-Z_][a-zA-Z0-9_.()]*$")) {
                    result = "#" + result;
                    logger.debug("Prefixed simple expression with #: {}", result);
                    return result;
                }
            }
        }
        
        // For complex expressions, find all variables and prefix them individually
        // But be more careful to avoid prefixing string literals and other non-variable tokens
        
        // First, protect quoted strings
        java.util.List<String> quotedStrings = new java.util.ArrayList<>();
        java.util.regex.Pattern quotedPattern = java.util.regex.Pattern.compile("'[^']*'");
        java.util.regex.Matcher quotedMatcher = quotedPattern.matcher(result);
        
        String protectedResult = result;
        int quotedIndex = 0;
        while (quotedMatcher.find()) {
            String quotedString = quotedMatcher.group();
            String placeholder = "QUOTED_" + quotedIndex++;
            quotedStrings.add(quotedString);
            protectedResult = protectedResult.replace(quotedString, placeholder);
        }
        
        // Protect T() system function calls and their property access
        java.util.List<String> systemFunctions = new java.util.ArrayList<>();
        java.util.regex.Pattern systemFunctionPattern = java.util.regex.Pattern.compile("T\\([^)]+\\)(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*");
        java.util.regex.Matcher systemFunctionMatcher = systemFunctionPattern.matcher(protectedResult);
        
        int systemIndex = 0;
        while (systemFunctionMatcher.find()) {
            String systemFunction = systemFunctionMatcher.group();
            String placeholder = "SYSTEM_" + systemIndex++;
            systemFunctions.add(systemFunction);
            protectedResult = protectedResult.replace(systemFunction, placeholder);
        }
        
        // Protect array access patterns like args[0], args[1].orderId, etc.
        // But first, prefix the array variable name
        java.util.List<String> arrayAccesses = new java.util.ArrayList<>();
        java.util.regex.Pattern arrayAccessPattern = java.util.regex.Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)\\[([^\\]]+)\\](?:\\.[a-zA-Z_][a-zA-Z0-9_.]*)*");
        java.util.regex.Matcher arrayAccessMatcher = arrayAccessPattern.matcher(protectedResult);
        
        int arrayIndex = 0;
        while (arrayAccessMatcher.find()) {
            String arrayAccess = arrayAccessMatcher.group();
            String arrayVar = arrayAccessMatcher.group(1);
            
            // Prefix the array variable name with # if it's not already prefixed
            String prefixedArrayAccess = arrayAccess;
            if (!arrayVar.startsWith("#")) {
                prefixedArrayAccess = arrayAccess.replaceFirst("\\b" + java.util.regex.Pattern.quote(arrayVar) + "\\b", "#" + arrayVar);
            }
            
            String placeholder = "ARRAY_ACCESS_" + arrayIndex++;
            arrayAccesses.add(prefixedArrayAccess);
            protectedResult = protectedResult.replace(arrayAccess, placeholder);
        }
        
        // CRITICAL FIX: Only prefix ROOT variables, not property names
        // This pattern matches variables that are:
        // 1. At the start of the string or after whitespace/operators
        // 2. Not already prefixed with #
        // 3. Not part of a protected placeholder
        // 4. Not preceded by a dot (to avoid prefixing property names)
        java.util.regex.Pattern variablePattern = java.util.regex.Pattern.compile("(?<![#a-zA-Z0-9_.])([a-zA-Z_][a-zA-Z0-9_]*)(?![a-zA-Z0-9_])");
        java.util.regex.Matcher variableMatcher = variablePattern.matcher(protectedResult);
        
        java.util.Set<String> variablesToPrefix = new java.util.HashSet<>();
        while (variableMatcher.find()) {
            String variable = variableMatcher.group(1);
            // Only prefix if it's not a SpEL keyword, operator, method name, or placeholder
            if (!isSpelKeyword(variable) && !isOperator(variable) && !isMethodName(variable) && 
                !variable.startsWith("QUOTED_") && !variable.startsWith("SYSTEM_") && !variable.startsWith("ARRAY_ACCESS_")) {
                variablesToPrefix.add(variable);
                logger.debug("Found variable to prefix: '{}'", variable);
            }
        }
        
        // Replace each variable with its prefixed version
        for (String variable : variablesToPrefix) {
            // Use a more precise replacement that avoids partial matches
            // Look for the variable at word boundaries or after operators
            String replacementPattern = "(?<![#a-zA-Z0-9_])" + java.util.regex.Pattern.quote(variable) + "(?![a-zA-Z0-9_])";
            protectedResult = protectedResult.replaceAll(replacementPattern, "#" + variable);
            logger.debug("Prefixed variable '{}' with # in expression: {}", variable, protectedResult);
        }
        
        // Restore array accesses
        for (int i = 0; i < arrayAccesses.size(); i++) {
            String placeholder = "ARRAY_ACCESS_" + i;
            protectedResult = protectedResult.replace(placeholder, arrayAccesses.get(i));
        }
        
        // Restore system functions
        for (int i = 0; i < systemFunctions.size(); i++) {
            String placeholder = "SYSTEM_" + i;
            protectedResult = protectedResult.replace(placeholder, systemFunctions.get(i));
        }
        
        // Restore quoted strings
        for (int i = 0; i < quotedStrings.size(); i++) {
            String placeholder = "QUOTED_" + i;
            protectedResult = protectedResult.replace(placeholder, quotedStrings.get(i));
        }
        
        result = protectedResult;
        
        logger.debug("Final result after variable prefixing: {}", result);
        return result;
    }
    
    private String convertBetweenOperator(String expression) {
        // Convert "X between Y and Z" to "X between {Y, Z}" for proper SpEL syntax
        // This is called from addVariablePrefixes, so no recursive calls
        
        // Replace "between X and Y" with "between {X, Y}"
        java.util.regex.Pattern betweenPattern = java.util.regex.Pattern.compile("between\\s+([^\\s]+)\\s+and\\s+([^\\s]+)");
        java.util.regex.Matcher betweenMatcher = betweenPattern.matcher(expression);
        
        String result = expression;
        if (betweenMatcher.find()) {
            String lowerBound = betweenMatcher.group(1);
            String upperBound = betweenMatcher.group(2);
            result = betweenMatcher.replaceFirst("between {" + lowerBound + ", " + upperBound + "}");
        }
        
        return result;
    }
    
    private boolean isSpelKeyword(String word) {
        // Common SpEL keywords that shouldn't get # prefix
        return word.equals("and") || word.equals("or") || word.equals("not") || 
               word.equals("true") || word.equals("false") || word.equals("null") ||
               word.equals("new") || word.equals("instanceof") || word.equals("matches") ||
               word.equals("between") || word.equals("div") || word.equals("mod") ||
               word.equals("T") || word.equals("args");
    }
    
    private boolean isOperator(String word) {
        // Common operators that shouldn't get # prefix
        return word.equals("and") || word.equals("or") || word.equals("not") ||
               word.equals("div") || word.equals("mod") || word.equals("matches") ||
               word.equals("between") || word.equals("instanceof");
    }
    
    private boolean isMethodName(String word) {
        // Common method names that should not be prefixed with #
        return word.equals("length") || word.equals("size") || word.equals("isEmpty") || 
               word.equals("contains") || word.equals("startsWith") || word.equals("endsWith") ||
               word.equals("toUpperCase") || word.equals("toLowerCase") || word.equals("trim") ||
               word.equals("substring") || word.equals("indexOf") || word.equals("lastIndexOf") ||
               word.equals("replace") || word.equals("split") || word.equals("equals") ||
               word.equals("hashCode") || word.equals("toString") || word.equals("getClass") ||
               word.equals("currentTimeMillis") || word.equals("nanoTime") || word.equals("getTime");
    }
    
    // Type conversion helper method
    private <T> T convertToExpectedType(Object value, Class<T> expectedType) {
        if (value == null) {
            return null;
        }
        
        // If already the correct type, return as is
        if (expectedType.isAssignableFrom(value.getClass())) {
            return expectedType.cast(value);
        }
        
        // Convert string representations to expected types
        String stringValue = value.toString();
        
        try {
            if (expectedType == String.class) {
                return expectedType.cast(stringValue);
            } else if (expectedType == Integer.class) {
                return expectedType.cast(Integer.valueOf(stringValue));
            } else if (expectedType == Long.class) {
                return expectedType.cast(Long.valueOf(stringValue));
            } else if (expectedType == Double.class) {
                return expectedType.cast(Double.valueOf(stringValue));
            } else if (expectedType == Boolean.class) {
                return expectedType.cast(Boolean.valueOf(stringValue));
            } else {
                return expectedType.cast(value);
            }
        } catch (NumberFormatException e) {
            logger.warn("Failed to convert '{}' to type {}", stringValue, expectedType.getSimpleName());
            return null;
        }
    }
    
    // Parameter discovery methods
    private String[] discoverParameterNamesRobustly(Method method) {
        String[] discoveredNames = null;
        
        // Strategy 1: Try Spring's default ParameterNameDiscoverer
        try {
            @SuppressWarnings("null")
            String[] paramNames = parameterNameDiscoverer.getParameterNames(method);
            logger.debug("Default ParameterNameDiscoverer result for {}: {}", method.getName(), java.util.Arrays.toString(paramNames));
            if (paramNames != null && paramNames.length > 0 && hasRealParameterNames(paramNames)) {
                logger.debug("Default ParameterNameDiscoverer found real parameter names for method: {}", method.getName());
                return paramNames;
            }
        } catch (Exception e) {
            logger.debug("Default ParameterNameDiscoverer failed for method {}: {}", method.getName(), e.getMessage());
        }
        
        // Strategy 2: Try StandardReflectionParameterNameDiscoverer
        try {
            org.springframework.core.StandardReflectionParameterNameDiscoverer standardDiscoverer = 
                new org.springframework.core.StandardReflectionParameterNameDiscoverer();
            discoveredNames = standardDiscoverer.getParameterNames(method);
            logger.debug("StandardReflectionParameterNameDiscoverer result for {}: {}", method.getName(), java.util.Arrays.toString(discoveredNames));
            if (discoveredNames != null && discoveredNames.length > 0 && hasRealParameterNames(discoveredNames)) {
                logger.debug("StandardReflectionParameterNameDiscoverer found real parameter names for method: {}", method.getName());
                return discoveredNames;
            }
        } catch (Exception e) {
            logger.debug("StandardReflectionParameterNameDiscoverer failed for method {}: {}", method.getName(), e.getMessage());
        }
        
        // Strategy 3: Direct reflection on Parameter objects (Java 8+)
        try {
            Parameter[] parameters = method.getParameters();
            if (parameters != null && parameters.length > 0) {
                discoveredNames = new String[parameters.length];
                boolean foundRealNames = false;
                
                for (int i = 0; i < parameters.length; i++) {
                    String paramName = parameters[i].getName();
                    
                    if (isRealParameterName(paramName)) {
                        discoveredNames[i] = paramName;
                        foundRealNames = true;
                        logger.debug("Direct reflection found parameter name: {} for method: {}", paramName, method.getName());
                    } else {
                        discoveredNames[i] = null;
                    }
                }
                
                if (foundRealNames) {
                    logger.debug("Direct reflection found real parameter names for method: {}", method.getName());
                    return discoveredNames;
                }
            }
        } catch (Exception e) {
            logger.debug("Direct reflection failed for method {}: {}", method.getName(), e.getMessage());
        }
        
        // Strategy 4: Try to extract from method signature analysis
        try {
            discoveredNames = extractParameterNamesFromSignature(method);
            if (discoveredNames != null && discoveredNames.length > 0 && hasRealParameterNames(discoveredNames)) {
                logger.debug("Signature analysis found real parameter names for method: {}", method.getName());
                return discoveredNames;
            }
        } catch (Exception e) {
            logger.debug("Signature analysis failed for method {}: {}", method.getName(), e.getMessage());
        }
        
        logger.debug("All parameter discovery strategies failed for method: {}", method.getName());
        return null;
    }
    
    private boolean hasRealParameterNames(String[] paramNames) {
        if (paramNames == null || paramNames.length == 0) {
            return false;
        }
        
        for (String paramName : paramNames) {
            if (isRealParameterName(paramName)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isRealParameterName(String paramName) {
        return paramName != null && 
               !paramName.equals("") &&
               paramName.length() > 0 &&
               Character.isJavaIdentifierStart(paramName.charAt(0)) && // Must be valid Java identifier
               !paramName.matches("arg\\d+"); // Filter out standard Java synthetic names (arg0, arg1, etc.)
    }
    
    private String[] extractParameterNamesFromSignature(Method method) {
        Parameter[] parameters = method.getParameters();
        if (parameters == null || parameters.length == 0) {
            return null;
        }
        
        String[] extractedNames = new String[parameters.length];
        boolean foundAny = false;
        
        for (int i = 0; i < parameters.length; i++) {
            Parameter param = parameters[i];
            
            // Only use real parameter names from the bytecode
            if (param.isNamePresent()) {
                String paramName = param.getName();
                if (isRealParameterName(paramName)) {
                    extractedNames[i] = paramName;
                    foundAny = true;
                    logger.debug("Extracted real parameter name: {} for method: {}", paramName, method.getName());
                }
            }
        }
        
        return foundAny ? extractedNames : null;
    }
}
