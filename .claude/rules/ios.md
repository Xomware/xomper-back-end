---
paths:
  - "*.swift"
  - "Sources/**"
  - "*.xcodeproj/**"
  - "*.xcworkspace/**"
---
# iOS Rules

- Check `ios-standards` skill for full Swift/SwiftUI conventions
- Use `@Observable` not `ObservableObject` (iOS 17+)
- Modern SwiftUI APIs: `foregroundStyle()`, `clipShape(.rect())`, `NavigationStack`
- MVVM: views are lightweight, logic lives in view models
- Async/await for all async work -- no completion handlers
- Accessibility: Dynamic Type, VoiceOver labels, 44pt touch targets
- No force unwrapping without documented justification
- Strict concurrency -- resolve all warnings
