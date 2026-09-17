import 'dart:async';

import '../core/signals.dart';
import 'signal.dart';
import 'state.dart';

/// {@template stream}
/// Stream signals wrap a standard asynchronous [Stream] and bridge it into the reactive state framework, exposing its emissions as a reactive [AsyncState].
///
/// You can construct a stream signal via the helper function [streamSignal] or by calling the `.toSignal()` extension method on any standard [Stream].
///
/// ### 1. Basic Stream Binding
/// ```dart
/// final s = streamSignal(() => countStream());
/// ```
///
/// Or via the extension:
/// ```dart
/// final s = countStream().toSignal();
/// ```
///
/// ### 2. Consuming stream emissions reactively
/// Reading `.value` on a [StreamSignal] returns an [AsyncState] object:
///
/// ```dart
/// effect(() {
///   s.value.map(
///     data: (val) => print('Stream emitted: $val'),
///     error: (err, stack) => print('Stream encountered error: $err'),
///     loading: () => print('Waiting for first stream emission...'),
///   );
/// });
/// ```
///
/// ### 3. Subscription Lifecycle and Manual Control
/// A stream signal automatically manages the underlying [StreamSubscription]. It listens when the signal has active subscribers and automatically cleans up/cancels when disposed to prevent memory leaks.
///
/// You can also manually control the subscription state:
/// - **`pause()`**: Pauses the underlying stream subscription.
/// - **`resume()`**: Resumes a paused subscription.
/// - **`cancel()`**: Cancels the subscription and marks the stream signal as done.
/// - **`isDone`**: Returns whether the stream has finished emitting or has been cancelled.
///
/// ```dart
/// final s = streamSignal(() => countStream());
/// s.pause(); // Temporarily halt stream values
/// ```
///
/// ### 4. Reactive Dependencies
/// Any reactive signals read synchronously inside the stream callback act as dependencies. When they mutate, the stream signal automatically cancels the current stream subscription, recreates a new stream using the updated values, and starts listening.
///
/// ```dart
/// final query = signal('flutter');
/// final s = streamSignal(() {
///   // Re-subscribes to a new database query stream every time the query changes!
///   return db.watchItems(query.value);
/// });
/// ```
/// {@endtemplate}
class StreamSignal<T> extends AsyncSignal<T> {
  /// {@template stream}
  /// Stream signals can be created by extension or method.
  ///
  /// ### streamSignal
  ///
  /// ```dart
  /// final stream = () async* {
  ///     yield 1;
  /// };
  /// final s = streamSignal(() => stream);
  /// ```
  ///
  /// ### toSignal()
  ///
  /// ```dart
  /// final stream = () async* {
  ///     yield 1;
  /// };
  /// final s = stream.toSignal();
  /// ```
  ///
  /// ## .value, .peek()
  ///
  /// Returns [`AsyncState<T>`](/dart/async/state) for the value and can handle the various states.
  ///
  /// The `value` getter returns the value of the stream if it completed successfully.
  ///
  /// > .peek() can also be used to not subscribe in an effect
  ///
  /// ```dart
  /// final stream = (int value) async* {
  ///     yield value;
  /// };
  /// final s = streamSignal(() => stream);
  /// final value = s.value.value; // 1 or null
  /// ```
  ///
  /// ## .reset()
  ///
  /// The `reset` method resets the stream to its initial state and starts a fresh subscription, restoring dependency tracking after cancellation.
  ///
  /// ```dart
  /// final stream = (int value) async* {
  ///     yield value;
  /// };
  /// final s = streamSignal(() => stream);
  /// s.reset();
  /// ```
  ///
  /// ## .refresh()
  ///
  /// Refresh the stream value by setting `isLoading` to true, but maintain the current state (AsyncData, AsyncLoading, AsyncError).
  ///
  /// ```dart
  /// final stream = (int value) async* {
  ///     yield value;
  /// };
  /// final s = streamSignal(() => stream);
  /// s.refresh();
  /// print(s.value.isLoading); // true
  /// ```
  ///
  /// ## .reload()
  ///
  /// Reload the stream value by setting the state to `AsyncLoading` and pass in the value or error as data.
  ///
  /// ```dart
  /// final stream = (int value) async* {
  ///     yield value;
  /// };
  /// final s = streamSignal(() => stream);
  /// s.reload();
  /// print(s.value is AsyncLoading); // true
  /// ```
  ///
  /// ## Dependencies
  ///
  /// By default the callback will be called once and the stream will be cached unless a signal is read in the callback.
  ///
  /// ```dart
  /// final count = signal(0);
  /// final s = streamSignal(() async* {
  ///     final value = count();
  ///     yield value;
  /// });
  ///
  /// await s.future; // 0
  /// count.value = 1;
  /// await s.future; // 1
  /// ```
  ///
  /// If there are signals that need to be tracked across an async gap then use the `dependencies` when creating the `streamSignal` to [`reset`](#.reset()) every time any signal in the dependency array changes.
  ///
  /// ```dart
  /// final count = signal(0);
  /// final s = streamSignal(
  ///     () async* {
  ///         final value = count();
  ///         yield value;
  ///     },
  ///     dependencies: [count],
  /// );
  /// s.value; // state with count 0
  /// count.value = 1; // resets the future
  /// s.value; // state with count 1
  /// ```
  /// @link https://dartsignals.dev/async/stream
  /// {@endtemplate}
  StreamSignal(
    Stream<T> Function() fn, {
    AsyncSignalOptions<T>? options,
    @Deprecated('Use options: AsyncSignalOptions(cancelOnError: ...) instead')
    bool? cancelOnError,
    @Deprecated('Use options: AsyncSignalOptions(initialValue: ...) instead')
    T? initialValue,
    @Deprecated('Use options: AsyncSignalOptions(dependencies: ...) instead')
    List<ReadonlySignal<dynamic>>? dependencies,
    @Deprecated('Use options: AsyncSignalOptions(onDone: ...) instead')
    void Function()? onDone,
    @Deprecated('Use options: AsyncSignalOptions(lazy: ...) instead')
    bool? lazy,
    @Deprecated('Use options: AsyncSignalOptions(autoDispose: ...) instead')
    bool? autoDispose,
    @Deprecated('Use options: AsyncSignalOptions(name: ...) instead')
    String? debugLabel,
  })  : _onDone = options?.onDone ?? onDone,
        cancelOnError = options?.cancelOnError ?? cancelOnError,
        dependencies = options?.dependencies ?? dependencies ?? const [],
        _factory = fn,
        _stream = computed(fn),
        super(
          (options?.initialValue ?? initialValue) != null
              ? AsyncState.data((options?.initialValue ?? initialValue) as T)
              : AsyncState.loading(),
          options: options ??
              AsyncSignalOptions<T>(
                autoDispose: autoDispose ?? false,
                name: debugLabel,
              ),
        ) {
    if (!(options?.lazy ?? lazy ?? true)) value;
  }

  final Stream<T> Function() _factory;
  Computed<Stream<T>> _stream;
  Stream<T>? _source;
  bool _fetching = false;
  StreamSubscription<T>? _subscription;
  final void Function()? _onDone;
  bool _done = false;
  EffectCleanup? _cleanup;
  EffectCleanup? _depCleanup;

  EffectCleanup _listenToDeps() {
    return untracked(() {
      if (dependencies.isEmpty) return () {};
      final cleanups = [
        for (final dependency in dependencies) _listenToDependency(dependency),
      ];
      return () {
        for (final c in cleanups) {
          c();
        }
      };
    });
  }

  EffectCleanup _listenToDependency(ReadonlySignal<dynamic> dependency) {
    Object? previous;
    return dependency.subscribe((value) {
      final before = previous;
      previous = value;
      if (before == null || before == value) return;
      if (dependency is AsyncSignal &&
          before is AsyncState &&
          value is AsyncState &&
          before.isLoading &&
          !value.isLoading) {
        return;
      }
      reset();
    });
  }

  /// Check if the signal is done
  bool get isDone => _done;

  /// Cancel the subscription on error
  late final bool? cancelOnError;

  /// List of dependencies to recompute the stream
  late final List<ReadonlySignal<dynamic>> dependencies;

  /// First value of the stream
  Future<T> get last => _stream.value.last;

  /// Last value of the stream
  Future<T> get first => _stream.value.first;

  /// Execute the stream
  Future<void> execute(Stream<T> src) async {
    if (disposed || _stream.disposed || _done || _fetching) return;
    final producer = _stream;
    final source = _source;
    _fetching = true;
    final subscription = src.listen(
      setValue,
      onError: setError,
      onDone: _finish,
      cancelOnError: cancelOnError,
    );
    // listen/onListen can synchronously cancel or restart this signal.
    if (!identical(producer, _stream) ||
        producer.disposed ||
        !identical(source, _source) ||
        _done ||
        disposed) {
      await subscription.cancel();
    } else {
      _subscription = subscription;
    }
  }

  Future<void>? _stopSubscription() {
    final subscription = _subscription;
    _subscription = null;
    _fetching = false;
    return subscription?.cancel();
  }

  void _stopTracking() {
    final cleanup = _cleanup;
    final depCleanup = _depCleanup;
    _cleanup = null;
    _depCleanup = null;
    _source = null;
    // A disposed producer distinguishes explicit cancellation from natural
    // completion, without disposing the writable AsyncState signal.
    _stream.dispose();
    cleanup?.call();
    depCleanup?.call();
  }

  Future<void> _finish() async {
    _done = true;
    final cancellation = _stopSubscription();
    _onDone?.call();
    await cancellation;
  }

  void _selectSource(Stream<T> src, {bool reset = true}) {
    if (disposed || _stream.disposed || identical(src, _source)) return;
    batch(() {
      _source = src;
      _stopSubscription();
      _done = false;
      if (reset) super.reset();
      init();
      execute(src);
    });
  }

  void _start({bool reset = true}) {
    if (_cleanup != null || disposed || _stream.disposed) return;
    final producer = _stream;
    // Reserve ownership before callbacks can synchronously read value again.
    _cleanup = () {};
    try {
      _selectSource(producer.peek(), reset: reset);
      if (producer.disposed || !identical(producer, _stream)) return;
      final cleanup = producer.subscribe(_selectSource);
      if (producer.disposed || !identical(producer, _stream)) {
        cleanup();
        return;
      }
      _cleanup = cleanup;
      _depCleanup = _listenToDeps();
    } catch (_) {
      if (identical(producer, _stream)) _stopTracking();
      rethrow;
    }
  }

  void _restart(void Function() updateState) {
    batch(() {
      _stopTracking();
      _stopSubscription();
      _done = false;
      // Keep the old producer disposed during state notifications: a reentrant
      // read must not initialize a second listener before this restart is ready.
      updateState();
      _stream = computed(_factory);
      _start(reset: false);
    });
  }

  /// Check if the subscription is paused
  bool get isPaused => _subscription?.isPaused ?? false;

  /// Pause the subscription
  void pause([Future<void>? resume]) {
    _subscription?.pause(resume);
    set(value, force: true);
  }

  /// Resume the subscription
  void resume() {
    _subscription?.resume();
    set(value, force: true);
  }

  /// Cancel the subscription and detach all producer/dependency observers.
  ///
  /// The current state remains readable and writable. Ordinary reads and
  /// dependency changes do not reconnect; [reset], [reload], or [refresh]
  /// explicitly restart the producer and restore dependency tracking.
  Future<void> cancel() async {
    _stopTracking();
    await _finish();
  }

  @override
  Future<void> reload() async {
    _restart(() => super.reload());
  }

  @override
  Future<void> refresh() async {
    _restart(() => super.refresh());
  }

  @override
  void reset([AsyncState<T>? value]) {
    _restart(() => super.reset(value));
  }

  @override
  void dispose() {
    _stopTracking();
    _stopSubscription();
    super.dispose();
  }

  @override
  AsyncState<T> get value {
    if (!disposed && !_stream.disposed) {
      _start();
      // The producer observer may run after a consumer that also reads the
      // identity. Pull its latest source before exposing the previous state.
      if (!_stream.disposed) _selectSource(_stream.peek());
    }
    return super.value;
  }

  @override
  void setError(Object error, [StackTrace? stackTrace]) {
    batch(() {
      super.setError(error, stackTrace);
      if (cancelOnError == true) {
        _finish();
      }
    });
  }
}

/// {@template stream}
/// Stream signals can be created by extension or method.
///
/// ### streamSignal
///
/// ```dart
/// final stream = () async* {
///     yield 1;
/// };
/// final s = streamSignal(() => stream);
/// ```
///
/// ### toSignal()
///
/// ```dart
/// final stream = () async* {
///     yield 1;
/// };
/// final s = stream.toSignal();
/// ```
///
/// ## .value, .peek()
///
/// Returns [`AsyncState<T>`](/dart/async/state) for the value and can handle the various states.
///
/// The `value` getter returns the value of the stream if it completed successfully.
///
/// > .peek() can also be used to not subscribe in an effect
///
/// ```dart
/// final stream = (int value) async* {
///     yield value;
/// };
/// final s = streamSignal(() => stream);
/// final value = s.value.value; // 1 or null
/// ```
///
/// ## .reset()
///
/// The `reset` method resets the stream to its initial state and starts a fresh subscription, restoring dependency tracking after cancellation.
///
/// ```dart
/// final stream = (int value) async* {
///     yield value;
/// };
/// final s = streamSignal(() => stream);
/// s.reset();
/// ```
///
/// ## .refresh()
///
/// Refresh the stream value by setting `isLoading` to true, but maintain the current state (AsyncData, AsyncLoading, AsyncError).
///
/// ```dart
/// final stream = (int value) async* {
///     yield value;
/// };
/// final s = streamSignal(() => stream);
/// s.refresh();
/// print(s.value.isLoading); // true
/// ```
///
/// ## .reload()
///
/// Reload the stream value by setting the state to `AsyncLoading` and pass in the value or error as data.
///
/// ```dart
/// final stream = (int value) async* {
///     yield value;
/// };
/// final s = streamSignal(() => stream);
/// s.reload();
/// print(s.value is AsyncLoading); // true
/// ```
///
/// ## Dependencies
///
/// By default the callback will be called once and the stream will be cached unless a signal is read in the callback.
///
/// ```dart
/// final count = signal(0);
/// final s = streamSignal(() async* {
///     final value = count();
///     yield value;
/// });
///
/// await s.future; // 0
/// count.value = 1;
/// await s.future; // 1
/// ```
///
/// If there are signals that need to be tracked across an async gap then use the `dependencies` when creating the `streamSignal` to [`reset`](#.reset()) every time any signal in the dependency array changes.
///
/// ```dart
/// final count = signal(0);
/// final s = streamSignal(
///     () async* {
///         final value = count();
///         yield value;
///     },
///     dependencies: [count],
/// );
/// s.value; // state with count 0
/// count.value = 1; // resets the future
/// s.value; // state with count 1
/// ```
/// @link https://dartsignals.dev/async/stream
/// {@endtemplate}
StreamSignal<T> streamSignal<T>(
  Stream<T> Function() callback, {
  AsyncSignalOptions<T>? options,
  @Deprecated('Use options: AsyncSignalOptions(initialValue: ...) instead')
  T? initialValue,
  @Deprecated('Use options: AsyncSignalOptions(dependencies: ...) instead')
  List<ReadonlySignal<dynamic>>? dependencies,
  @Deprecated('Use options: AsyncSignalOptions(onDone: ...) instead')
  void Function()? onDone,
  @Deprecated('Use options: AsyncSignalOptions(cancelOnError: ...) instead')
  bool? cancelOnError,
  @Deprecated('Use options: AsyncSignalOptions(lazy: ...) instead') bool? lazy,
  @Deprecated('Use options: AsyncSignalOptions(autoDispose: ...) instead')
  bool? autoDispose,
  @Deprecated('Use options: AsyncSignalOptions(name: ...) instead')
  String? debugLabel,
}) {
  return StreamSignal(
    callback,
    options: (options ?? AsyncSignalOptions<T>()).copyWith(
      initialValue: initialValue,
      dependencies: dependencies,
      onDone: onDone,
      cancelOnError: cancelOnError,
      lazy: lazy,
      autoDispose: autoDispose,
      name: debugLabel,
    ),
  );
}
