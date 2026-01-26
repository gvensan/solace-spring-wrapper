# Contributing to Solace Spring Wrapper

Thank you for your interest in contributing to the Solace Spring Wrapper project! This document provides guidelines and information for contributors.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone.

## How to Contribute

### Reporting Issues

Before creating an issue, please:

1. Search existing issues to avoid duplicates
2. Use a clear and descriptive title
3. Provide detailed information including:
   - Steps to reproduce the issue
   - Expected vs actual behavior
   - Environment details (Java version, Spring Boot version, Solace broker version)
   - Relevant logs or stack traces

### Suggesting Enhancements

Enhancement suggestions are welcome! Please include:

- A clear description of the enhancement
- The use case or problem it solves
- Any potential implementation approaches

### Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Write clear, concise commit messages**
3. **Include tests** for new functionality
4. **Update documentation** as needed
5. **Ensure all tests pass** before submitting

#### Pull Request Process

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes with appropriate tests
3. Run the test suite: `mvn clean test`
4. Push to your fork and submit a pull request
5. Wait for review and address any feedback

## Development Setup

### Prerequisites

- JDK 18 or later
- Maven 3.6+
- Access to a Solace PubSub+ broker (local or cloud) for integration tests

### Building the Project

```bash
# Compile
mvn clean compile

# Run unit tests
mvn test

# Run integration tests (requires broker)
mvn -Pintegration verify

# Build package
mvn clean package
```

### Code Style Guidelines

- Follow standard Java naming conventions
- Use meaningful variable and method names
- Keep methods focused and reasonably sized
- Add Javadoc comments for public APIs
- Avoid unnecessary complexity

### Testing Requirements

- Write unit tests for new functionality
- Maintain or improve code coverage
- Integration tests should handle broker unavailability gracefully
- Use descriptive test method names

## Project Structure

```
src/
├── main/java/com/solace/wrapper/
│   ├── annotation/          # Annotations and processors
│   ├── config/              # Configuration classes
│   ├── connection/          # Connection management
│   ├── consumer/            # Consumer implementation
│   ├── exception/           # Custom exceptions
│   ├── health/              # Health indicators
│   ├── publisher/           # Publisher implementation
│   └── serialization/       # Message serialization
└── test/java/               # Test classes
```

## Questions?

If you have questions about contributing, feel free to open an issue for discussion.

## License

By contributing to this project, you agree that your contributions will be licensed under the Apache License 2.0.
