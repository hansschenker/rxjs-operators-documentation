# RxJS Operator Documentation

> **Comprehensive, formal specifications for RxJS operators using an eight-policy framework**

A systematic documentation project providing complete, pedagogically-sound operator specifications for the RxJS library. Each operator is documented following a rigorous eight-policy framework designed to eliminate ambiguity, prevent common mistakes, and provide developers with complete mental models for reactive programming.

**Project Mentor**: [Claude Sonnet 4.5](https://www.anthropic.com/claude) - AI assistant by Anthropic

---

## 📚 Documentation Philosophy

This project goes beyond typical API documentation by providing:

- **Mathematical precision** - Formal functional specifications
- **Visual clarity** - ASCII marble diagrams for temporal behavior
- **Type safety** - Complete TypeScript integration details
- **Practical wisdom** - Real-world examples and anti-patterns
- **Pedagogical structure** - Designed for both learning and reference

### The Eight-Policy Framework

Every operator is documented using eight comprehensive policies:

1. **Operator Identity** - Classification, categorization, and signatures
2. **Functional Specification** - Mathematical transformation definition
3. **Marble Diagrams** - Visual temporal behavior representation
4. **Behavioral Characteristics** - Subscription, completion, errors, backpressure
5. **Type System Integration** - TypeScript type safety and inference
6. **Practical Examples** - Basic usage, common patterns, edge cases
7. **Common Pitfalls** - Anti-patterns with corrections
8. **Related Operators** - Ecosystem context and alternatives

See [SKILL.md](./SKILL.md) for the complete documentation standard.

---

## 🎯 Project Goals

1. **Eliminate ambiguity** in operator behavior through formal specifications
2. **Prevent common mistakes** by documenting anti-patterns and pitfalls
3. **Accelerate learning** with systematic categorization and learning paths
4. **Improve debugging** by providing complete behavioral characteristics
5. **Support teaching** with cognitive load analysis and pedagogical notes
6. **Enable informed decisions** through comprehensive operator comparisons

---

## 📖 Documentation Status

**Current Progress**: 2 / 100+ operators documented

### Recently Documented
- ✅ [combineLatest](./operators/combination/combineLatest.md) - Combination operator for reactive state management
- ✅ [mergeMap](./operators/transformation/mergeMap.md) - Concurrent flattening for parallel async operations

### Browse All Operators
See the complete [Operator Index](./index.md) for:
- All operator categories and status
- Cognitive load and usage frequency ratings
- Learning path recommendations
- Quick reference guides

---

## 🚀 Quick Start

### For Learners

**New to RxJS?** Follow the [Beginner Learning Path](./index.md#beginner-path):
1. Start with creation operators (of, from, fromEvent)
2. Master basic transformations (map, filter, tap)
3. Learn combination patterns (combineLatest)
4. Understand timing (debounceTime, throttleTime)
5. Practice completion (take, takeUntil, first)

**Each operator documentation includes**:
- Clear explanations of what it does and why
- Visual marble diagrams
- Runnable code examples
- Common mistakes to avoid
- When to use this vs. alternatives

### For Practitioners

**Looking for a specific operator?** Use the [Quick Reference](./index.md#quick-reference) or search by:
- **Use case** - "I need to handle multiple HTTP requests"
- **Problem** - "My stream is emitting too fast"
- **Pattern** - "I need sequential async operations"

**Key operator families documented**:
- Flattening strategies: mergeMap, switchMap (coming soon), concatMap (coming soon)
- Rate limiting: debounceTime (coming soon), throttleTime (coming soon)
- Combination: combineLatest, withLatestFrom (coming soon)

### For Educators

Each operator includes:
- **Cognitive Load Rating** (1-5) - Helps sequence curriculum
- **Usage Frequency** (1-5) - Prioritizes essential operators
- **Teaching Notes** - Prerequisite concepts and common misconceptions
- **rxjs-strategies Integration** - Strategy pattern classifications

---

## 📂 Repository Structure

```
rxjs-operator-documentation/
│
├── README.md                    # This file
├── SKILL.md                     # Documentation standard and template
├── index.md                     # Complete operator index and learning paths
├── CONTRIBUTING.md              # Contribution guidelines (coming soon)
│
├── operators/                   # Operator documentation by category
│   ├── combination/
│   │   └── combineLatest.md
│   ├── transformation/
│   │   └── mergeMap.md
│   ├── filtering/
│   ├── error-handling/
│   ├── utility/
│   └── ... (other categories)
│
└── examples/                    # Standalone examples (coming soon)
    ├── patterns/
    └── anti-patterns/
```

---

## 🎓 Learning Resources

### Understanding the Framework

Before diving into specific operators, review:
- [SKILL.md](./SKILL.md) - Complete documentation methodology
- [Operator Index](./index.md) - Categorization system and learning paths

### External Resources

- **RxJS Official Docs**: [https://rxjs.dev](https://rxjs.dev)
- **ReactiveX**: [http://reactivex.io](http://reactivex.io)
- **Learn RxJS**: [https://www.learnrxjs.io](https://www.learnrxjs.io)
- **RxJS Marbles**: [https://rxmarbles.com](https://rxmarbles.com)

### Related Projects

This documentation project is part of a larger ecosystem:
- **rxjs-strategies** - Library implementing strategy patterns for RxJS operators
- **rxjs-contracts** - Design by Contract debugging library for Observables

---

## 🤝 Contributing

We welcome contributions! Here's how you can help:

### Documentation Contributions

1. **Document new operators** following the [SKILL.md](./SKILL.md) template
2. **Improve existing documentation** with better examples or clarifications
3. **Add practical examples** from real-world use cases
4. **Report issues** with existing documentation

### Quality Standards

All contributions must:
- ✅ Follow the eight-policy framework completely
- ✅ Include minimum 3 practical examples
- ✅ Document at least 2 common pitfalls
- ✅ Provide accurate marble diagrams
- ✅ Include complete TypeScript signatures
- ✅ Test all code examples

See [CONTRIBUTING.md](./CONTRIBUTING.md) (coming soon) for detailed guidelines.

### Suggesting Operators to Document

Priority is given to:
1. **High-frequency operators** - Most commonly used in production
2. **Confusing operators** - Those with common misconceptions
3. **Operator families** - Completing related sets (e.g., all flattening operators)
4. **Teaching sequences** - Operators needed for learning paths

Open an issue to suggest which operator should be documented next!

---

## 🏗️ Project History

### Genesis

This project emerged from the need for comprehensive, formal operator documentation that serves both as:
- A **reference** for experienced developers debugging complex reactive streams
- An **educational resource** for developers learning functional reactive programming
- A **specification** for library authors and tool creators

### Mentorship by Claude Sonnet 4.5

This documentation project is mentored and co-created with **Claude Sonnet 4.5**, Anthropic's advanced AI assistant. Claude contributes:

- **Template design** - The eight-policy framework structure
- **Documentation generation** - Comprehensive operator specifications
- **Quality assurance** - Ensuring consistency and completeness
- **Pedagogical insights** - Learning path design and cognitive load analysis
- **Technical accuracy** - Verification against RxJS source code and TypeScript

The human-AI collaboration enables:
1. **Systematic coverage** - Consistent application of the framework across all operators
2. **Deep analysis** - Exploration of edge cases and anti-patterns
3. **Rapid iteration** - Quick generation of comprehensive documentation
4. **Quality at scale** - Maintaining high standards across 100+ operators

### Integration with rxjs-strategies

This documentation serves as the foundational knowledge base for the **rxjs-strategies** library, which:
- Applies strategy patterns to simplify RxJS's 100+ operators
- Reduces cognitive complexity through systematic categorization
- Provides educational tools for teaching reactive programming
- Implements Design by Contract debugging with **rxjs-contracts**

---

## 📊 Operator Coverage Roadmap

### Phase 1: Core Operators (Q1 2025)
**Target**: 20 most frequently used operators

- [x] combineLatest
- [x] mergeMap
- [ ] switchMap
- [ ] map
- [ ] filter
- [ ] tap
- [ ] catchError
- [ ] debounceTime
- [ ] distinctUntilChanged
- [ ] takeUntil
- [ ] ... (10 more)

### Phase 2: Essential Patterns (Q2 2025)
**Target**: Complete operator families

- [ ] All flattening operators (concatMap, exhaustMap, mergeAll, etc.)
- [ ] All rate limiting operators (throttle, audit, sample families)
- [ ] All combination operators (zip, forkJoin, withLatestFrom, etc.)
- [ ] All error handling operators (retry, retryWhen, onErrorResumeNext)

### Phase 3: Complete Coverage (Q3-Q4 2025)
**Target**: All RxJS 7.x operators

- [ ] Utility operators
- [ ] Conditional operators
- [ ] Mathematical operators
- [ ] Multicasting operators
- [ ] Creation operators

---

## 🔬 Methodology

### Documentation Process

Each operator undergoes:

1. **Source Analysis** - Review of RxJS implementation and TypeScript definitions
2. **Behavioral Testing** - Verification of edge cases and completion semantics
3. **Pattern Identification** - Analysis of common use cases and anti-patterns
4. **Framework Application** - Systematic documentation using eight policies
5. **Cross-Reference** - Comparison with related operators and alternatives
6. **Pedagogical Review** - Assessment of cognitive load and teaching placement

### Quality Assurance

Documentation is verified through:
- ✅ Code example testing (all examples must run)
- ✅ Marble diagram validation (behavioral correctness)
- ✅ TypeScript compilation (signature accuracy)
- ✅ Cross-referencing (related operators are accurate)
- ✅ Completeness check (all eight policies covered)

---

## 📄 License

This documentation is released under the **MIT License**.

```
Copyright (c) 2024-2025 RxJS Operator Documentation Project

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## 🙏 Acknowledgments

- **RxJS Core Team** - For creating and maintaining the RxJS library
- **ReactiveX Community** - For reactive programming patterns and documentation
- **Anthropic** - For developing Claude Sonnet 4.5, the AI mentor of this project
- **TypeScript Team** - For type system enabling precise specifications
- **Functional Programming Community** - For theoretical foundations

---

## 📞 Contact & Support

- **Issues**: Open an issue on GitHub for bugs, suggestions, or questions
- **Discussions**: Use GitHub Discussions for general questions about RxJS or reactive programming
- **Related Work**: See [rxjs-strategies](https://github.com/[your-username]/rxjs-strategies) and [rxjs-contracts](https://github.com/[your-username]/rxjs-contracts)

---

## 🌟 Star History

If you find this documentation valuable, please consider starring the repository to help others discover it!

---

**Documentation Standard**: Eight-Policy Framework v1.0  
**RxJS Version Compatibility**: 7.x, 8.x  
**Last Updated**: December 2024  
**Project Mentor**: Claude Sonnet 4.5 by Anthropic
