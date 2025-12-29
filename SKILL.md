# RxJS Operator Documentation

## Overview
This skill provides a comprehensive, formal specification structure for documenting RxJS operators. It applies an eight-policy framework designed to eliminate ambiguity in operator behavior, prevent common pipeline composition errors, and provide complete mental models for reactive programming operators.

The framework serves both reference and educational purposes, ensuring consistency across operator documentation while reducing cognitive complexity through systematic categorization.

## When to use this skill
- User requests documentation for any RxJS operator
- User asks to "document", "describe", "explain", or "specify" an RxJS operator
- User mentions creating operator reference materials
- User needs educational content about reactive programming operators
- User is working on operator specifications for library development
- User asks about operator behavior, characteristics, or usage patterns

**Trigger phrases:**
- "Document the [operator] operator"
- "Describe how [operator] works"
- "Create operator documentation for [operator]"
- "Explain the [operator] operator"
- "Write a specification for [operator]"
- "What does [operator] do?"

## Core Documentation Policies

This skill implements an eight-policy framework that must be applied to every operator documentation:

### Policy 1: Operator Identity
**Purpose**: Establish clear classification and categorization

**Required elements:**
- **Name**: Official operator name as it appears in RxJS API
- **Category**: Classification within the 17-group operator taxonomy:
  - Creation Operators
  - Transformation Operators
  - Filtering Operators
  - Combination Operators
  - Multicasting Operators
  - Error Handling Operators
  - Utility Operators
  - Conditional Operators
  - Mathematical/Aggregate Operators
  - Join Creation Operators
  - Join Operators
  - Rate Limiting Operators
  - Higher-Order Operators
  - Connectable Observable Operators
  - Testing/Debugging Operators
  - Interop Operators
  - Subject/Notification Operators
- **Type**: Specific operator classification (e.g., "Higher-order transformation", "Rate limiting")
- **Import path**: Complete TypeScript import statement
- **Signature**: Full TypeScript signature with all generic parameters

**Format:**
```markdown
## Identity
- **Name**: {operator name}
- **Category**: {one of 17 categories}
- **Type**: {specific classification}
- **Import**: 
  ```typescript
  import { {name} } from 'rxjs' | 'rxjs/operators';
  ```
- **Signature**: 
  ```typescript
  {complete TypeScript signature with generics}
  ```
```

### Policy 2: Functional Specification
**Purpose**: Define the mathematical/functional transformation precisely

**Required elements:**
- **Input**: Type and semantic description of source Observable(s)
- **Output**: Type and semantic description of result Observable
- **Transformation**: Precise description of the mapping/transformation function
- **Mathematical representation**: Formal notation when applicable
- **Invariants**: Properties that remain true throughout execution

**Format:**
```markdown
## Functional Specification

**Input**: {Observable type and semantics}

**Output**: {Observable type and semantics}

**Transformation**: {precise functional description}

**Mathematical representation**:
```
{formal notation if applicable}
```

**Invariants**:
- {invariant 1}
- {invariant 2}
- {invariant n}
```

**Key principles:**
- Use mathematical notation for precision (∀, ∃, →, etc.)
- Describe cardinality (1-to-1, 1-to-many, many-to-1)
- Specify timing characteristics (synchronous/asynchronous)
- Define memory/space complexity

### Policy 3: Marble Diagram
**Purpose**: Visual representation of temporal behavior

**Required elements:**
- **Source stream(s)**: ASCII marble diagram showing input timeline(s)
- **Operator application**: Clear indication of operator name and parameters
- **Result stream**: ASCII marble diagram showing output timeline
- **Legend**: Symbol definitions for values, completion, error, subscription
- **Time axis**: Consistent spacing representing time units

**Format:**
```markdown
## Marble Diagram

```
Source:   {ASCII timeline}
            {operator(params)}
Result:   {ASCII timeline}

Legend:
  - : time unit (10ms)
  a,b,c : emitted values
  | : completion
  # : error
  ^ : subscription point
```

**Key observation**: {Critical insight from the diagram}
```

**ASCII Symbols:**
- `-` = time unit (typically 10ms)
- Letters/numbers = emitted values
- `|` = successful completion
- `#` = error
- `^` = subscription point
- `(ab)` = synchronous emissions
- Spaces = align events temporally

### Policy 4: Behavioral Characteristics
**Purpose**: Document runtime behavior and edge cases

**Required sections:**

1. **Subscription behavior**:
   - When does the operator subscribe to source?
   - Does it create multiple subscriptions?
   - Hot vs. cold implications
   - Eager vs. lazy evaluation

2. **Completion semantics**:
   - How does source completion propagate?
   - Does the operator complete independently?
   - Conditions for premature completion
   - Handling of empty observables

3. **Error handling**:
   - How are errors propagated?
   - Does the operator catch/transform errors?
   - Recovery mechanisms
   - Error boundary behavior

4. **Backpressure**:
   - How does the operator handle fast producers?
   - Does it buffer, drop, or throttle emissions?
   - Memory implications
   - Rate limiting characteristics

**Format:**
```markdown
## Behavioral Characteristics

**Subscription**: 
{subscription timing and mechanism}

**Completion semantics**:
{completion propagation rules and edge cases}

**Error handling**:
{error propagation and handling mechanisms}

**Backpressure**:
{buffering, rate limiting, and memory behavior}

**Hot vs. Cold**:
{implications for hot/cold observables}
```

### Policy 5: Type System Integration
**Purpose**: Precise TypeScript type definitions and constraints

**Required elements:**
- **Generic parameters**: All type variables with their constraints
- **Input types**: Complete type of source Observable(s)
- **Output type**: Complete type of result Observable
- **Type narrowing**: How the operator affects type inference
- **Type safety guarantees**: What the compiler can verify

**Format:**
```markdown
## Type System Integration

```typescript
/**
 * Type Parameters:
 *   {list all generics with descriptions and constraints}
 * 
 * Input Types:
 *   {complete input type specifications}
 * 
 * Output Type:
 *   {complete output type specification}
 * 
 * Type Narrowing:
 *   {how types are refined or transformed}
 * 
 * Type Safety:
 *   {compile-time guarantees provided}
 */

// Example: Type preservation demonstration
{concrete type example showing inference}
```
```

**Key considerations:**
- Show how generic constraints work
- Demonstrate type inference in practice
- Highlight type safety benefits
- Show potential type pitfalls

### Policy 6: Practical Examples
**Purpose**: Demonstrate real-world usage patterns

**Required examples (minimum 3):**

1. **Basic Usage**: Simplest possible use case demonstrating core functionality
2. **Common Pattern**: Typical production scenario showing real-world application
3. **Edge Cases**: Handling errors, empty streams, or complex conditions

**Format:**
```markdown
## Examples

### Basic Usage
```typescript
{minimal working example with clear comments}
// Output: {expected result}
```

### Common Pattern - {Pattern Name}
```typescript
{realistic production scenario}
// Demonstrates: {what this shows}
```

### Edge Cases - {Case Description}
```typescript
{error handling, empty streams, complex conditions}
// Shows: {edge case being handled}
```

### Advanced Pattern - {Pattern Name} (optional)
```typescript
{complex composition or integration pattern}
```
```

**Example quality standards:**
- All code must be complete and runnable
- Include expected output in comments
- Show imports explicitly
- Use realistic variable names
- Demonstrate type safety
- Progress from simple to complex

### Policy 7: Common Pitfalls
**Purpose**: Document frequent misuse and anti-patterns

**Required sections:**

1. **Anti-patterns**: Common mistakes with corrections
2. **Performance considerations**: Computational and memory implications
3. **Composition errors**: Incorrect operator ordering or type mismatches

**Format:**
```markdown
## Common Pitfalls

### Anti-pattern: {Descriptive Name}
```typescript
// ❌ INCORRECT
{problematic code}

// ✅ CORRECT
{corrected code}

// WHY: {explanation of the problem}
// {Additional context}
```

### Performance: {Consideration Name}
**When this matters**: 
{scenarios where this is relevant}

**What to do**:
```typescript
{optimization approach}
```
```

**Anti-pattern structure:**
- Show the incorrect approach first (marked with ❌)
- Show the correct approach (marked with ✅)
- Explain WHY the first approach fails
- Provide context on WHEN to use the correct approach

### Policy 8: Related Operators
**Purpose**: Provide context within the operator ecosystem

**Required sections:**

1. **Same category**: Operators with similar functionality
2. **Complementary operators**: Operators commonly used together
3. **Alternatives**: Different approaches to same problem

**Format:**
```markdown
## Related Operators

**Same Category ({Category Name})**:
- **`operator1`**: {how it differs, when to use instead}
- **`operator2`**: {how it differs, when to use instead}

**Complementary Operators**:
- **`operator`**: {typical composition pattern}
- **`operator`**: {how they work together}

**Alternatives by Use Case**:

| Use Case | Instead of {thisOp} | Use This | Why |
|----------|---------------------|----------|-----|
| {scenario} | {current approach} | {alternative} | {rationale} |

**Migration Notes** (if applicable):
```typescript
// Deprecated API
{old approach}

// Current API
{new approach}
```
```

## Documentation Template Structure

When documenting an operator, use this complete structure:

```markdown
# {Operator Name}

## Identity
- **Name**: {official name}
- **Category**: {one of 17 categories}
- **Type**: {specific classification}
- **Import**: `import { {name} } from 'rxjs/operators'`
- **Signature**: 
  ```typescript
  {full TypeScript signature}
  ```

## Functional Specification

**Input**: {Observable type and semantics}

**Output**: {Observable type and semantics}

**Transformation**: {mathematical/functional description}

**Mathematical representation**:
```
{formal notation if applicable}
```

**Invariants**:
- {property 1}
- {property 2}
- {property n}

## Marble Diagram

```
{source diagram}
  {operator with params}
{result diagram}

Legend:
  - : time unit
  a,b : values
  | : completion
  # : error
```

**Key observation**: {Critical insight}

## Behavioral Characteristics

**Subscription**: {subscription mechanism}

**Completion semantics**: {completion rules}

**Error handling**: {error behavior}

**Backpressure**: {rate limiting/buffering}

**Hot vs. Cold**: {implications}

## Type System Integration

```typescript
/**
 * Type Parameters: {descriptions}
 * Input Types: {specifications}
 * Output Type: {specification}
 * Type Narrowing: {inference details}
 * Type Safety: {guarantees}
 */

{type example}
```

## Examples

### Basic Usage
```typescript
{minimal example}
```

### Common Pattern - {Name}
```typescript
{production scenario}
```

### Edge Cases - {Name}
```typescript
{edge case handling}
```

## Common Pitfalls

### Anti-pattern: {Name}
```typescript
// ❌ INCORRECT
{bad code}

// ✅ CORRECT
{good code}

// WHY: {explanation}
```

### Performance: {Consideration}
{guidance}

## Related Operators

**Same Category**: {list with comparisons}

**Complementary**: {composition patterns}

**Alternatives**: {trade-offs table}

## References
- **RxJS Official Docs**: {link}
- **ReactiveX Documentation**: {link}
- **Source Code**: {GitHub link}

---

## Additional Notes for rxjs-strategies Integration

**Strategy Classification**: 
- **Pattern**: {strategy pattern name}
- **Cognitive Load**: {1-5 rating}
- **Usage Frequency**: {1-5 rating}
- **Composability**: {1-5 rating}

**Problem Domain**: 
{what problem this operator solves}

**When to Teach**: 
{pedagogical placement in curriculum}
```

## Quality Standards

### Completeness Checklist
When documenting an operator, verify:
- [ ] All 8 policies addressed comprehensively
- [ ] Minimum 3 practical examples included
- [ ] Marble diagram accurately represents behavior
- [ ] Type signatures are complete and correct
- [ ] At least 2 anti-patterns documented with corrections
- [ ] Related operators listed with clear comparisons
- [ ] All code examples tested and verified

### Accuracy Requirements
- Verify behavior against current RxJS source code (v7+)
- Test all code examples for correctness
- Validate marble diagrams (ideally with rxjs-marbles or manual testing)
- Check TypeScript signatures compile without errors
- Cross-reference with official RxJS documentation
- Verify version-specific behavior notes

### Pedagogical Quality Standards
- **Explain "why" not just "what"**: Provide rationale for design decisions
- **Progress from simple to complex**: Examples should build understanding incrementally
- **Use consistent terminology**: Align with functional programming and RxJS conventions
- **Relate to theory**: Connect to functional programming principles where applicable
- **Provide context**: Explain when to use vs. when not to use
- **Anticipate confusion**: Address common misconceptions proactively

### Writing Style Guidelines
- Use precise, unambiguous language
- Prefer active voice over passive
- Use present tense for operator behavior
- Include concrete examples over abstract descriptions
- Balance formality with accessibility
- Maintain consistent section structure

## Integration with rxjs-strategies Library

When documenting operators for the rxjs-strategies library context, include additional analysis:

### Strategy Classification
**Pattern identification**:
- Which strategy pattern does this operator exemplify?
- How does it reduce cognitive complexity compared to imperative approaches?
- What reactive programming problem domain does it address?

### Cognitive Load Analysis
Rate each operator on these dimensions (1-5 scale):

**Conceptual Complexity** (1=simple, 5=complex):
- How difficult is the core concept?
- How many prerequisite concepts are required?
- How counterintuitive is the behavior?

**Common Usage Frequency** (1=rare, 5=ubiquitous):
- How often is this needed in production code?
- Is this a fundamental building block?
- What percentage of RxJS codebases use this?

**Composability Factor** (1=isolated, 5=highly composable):
- How well does it combine with other operators?
- How many common patterns include this operator?
- Does it create or solve composition challenges?

### Teaching Sequence Placement
Specify where in the curriculum this operator belongs:
- **Prerequisites**: What must students learn first?
- **Teaches**: What concepts does mastering this operator unlock?
- **Leads to**: What advanced patterns build on this?
- **Common with**: What operators are typically learned together?

## Special Considerations by Operator Category

### For Creation Operators
- Emphasize when/why to create observables this way
- Compare to alternative creation methods
- Discuss hot vs. cold implications explicitly
- Show subscription timing clearly

### For Transformation Operators
- Highlight pure vs. impure transformations
- Show cardinality changes (1-to-1, 1-to-many, etc.)
- Demonstrate type transformations clearly
- Compare to similar transformations

### For Combination Operators
- Clarify completion semantics with multiple sources
- Show synchronization behavior in marble diagrams
- Compare different combination strategies
- Address performance with many sources

### For Error Handling Operators
- Show error propagation paths clearly
- Demonstrate recovery strategies
- Explain when/how streams terminate
- Compare error handling approaches

### For Higher-Order Operators
- Explain flattening strategies (concat, merge, switch)
- Show concurrency implications
- Demonstrate cancellation behavior
- Compare flattening policies

## References and Resources

### Official Documentation
- RxJS Official Docs: https://rxjs.dev
- ReactiveX Documentation: http://reactivex.io
- RxJS GitHub Repository: https://github.com/ReactiveX/rxjs

### Theoretical Foundations
- Functional Reactive Programming (FRP) principles
- Category Theory foundations for reactive streams
- Observer pattern and reactive programming paradigms

### Testing and Validation Tools
- rxjs-marbles: Marble diagram testing
- RxJS Testing utilities: TestScheduler
- TypeScript Playground: Type checking

### Additional Context
When documenting operators, consider referencing:
- TC39 Observable Proposal for JavaScript observables
- Academic papers on reactive programming
- Real-world case studies from production applications
- Performance benchmarks for operator comparisons

## Notes on Application

### Consistency Across Documentation
- Use identical section structure for all operators
- Maintain consistent terminology and notation
- Use the same marble diagram symbols
- Format code examples identically
- Apply the same quality standards

### Handling Operator Variations
Some operators have multiple forms (creation vs. pipeable, deprecated vs. current):
- Document the current, recommended form primarily
- Include migration notes for deprecated forms
- Show both forms if commonly encountered
- Clearly mark deprecated approaches

### Dealing with Complex Operators
For operators with many overloads or complex behavior:
- Start with the most common use case
- Document variations systematically
- Use subsections for different forms
- Provide decision trees for choosing variations

### Educational vs. Reference Balance
This template serves both purposes:
- **For learning**: Follow the examples section-by-section
- **For reference**: Jump directly to needed policy section
- **For debugging**: Focus on behavioral characteristics and pitfalls
- **For design**: Consult related operators and alternatives

## Success Criteria

Documentation following this skill should enable users to:

1. **Understand** the operator's purpose and behavior completely
2. **Apply** the operator correctly in common scenarios
3. **Avoid** common pitfalls and anti-patterns
4. **Debug** issues arising from operator misuse
5. **Choose** between this operator and alternatives appropriately
6. **Compose** the operator effectively in pipelines
7. **Predict** edge case behavior without experimentation
8. **Explain** the operator to others clearly

If documentation achieves these goals, it meets the quality standard defined by this skill.

## Prompt Examples for Applying This Skill

Users can trigger this skill with prompts like:

- "Document the switchMap operator"
- "Create comprehensive documentation for mergeMap"
- "Explain the debounceTime operator using the formal template"
- "I need full operator specs for concatMap"
- "Write educational material for the scan operator"

Claude should recognize these as requests to apply the full eight-policy framework and generate complete, formal operator documentation following all standards defined in this skill.
