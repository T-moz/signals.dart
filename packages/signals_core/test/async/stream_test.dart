import 'dart:async';

import 'package:signals_core/signals_core.dart';
import 'package:test/test.dart';

void main() {
  SignalsObserver.instance = null;
  group('Stream', () {
    test('streamSignal', () async {
      final stream = _stream();
      final signal = streamSignal(() => stream);
      expect(signal.peek().isLoading, true);

      final completer = Completer<int>();
      effect(() {
        signal.value;
        if (signal.value.hasValue) {
          completer.complete(signal.peek().requireValue);
        }
      });
      final result = await completer.future;

      expect(result, 10);
    });

    test('extension on Stream', () async {
      final stream = _stream();
      final s = stream.toStreamSignal();
      expect(s.peek().isLoading, true);

      final completer = Completer<int>();
      effect(() {
        s.value;
        if (s.value.hasValue) {
          completer.complete(s.peek().requireValue);
        }
      });
      final result = await completer.future;

      expect(result, 10);
    });

    test('dependencies', () async {
      final prefix = signal('a');
      final count = signal(1);
      final s = streamSignal(() => _string(prefix(), count()));
      expect(s.peek().isLoading, true);

      var result = await s.future;
      expect(result, 'a0');

      prefix.value = 'b';
      result = await s.future;
      expect(result, 'b0');
    });

    test('check reload calls', () async {
      int calls = 0;

      Stream<int> stream() async* {
        calls++;
        await Future.delayed(const Duration(milliseconds: 5));
        yield* _stream();
      }

      final signal = streamSignal(() => stream());
      expect(signal.peek().isLoading, true);
      expect(calls, 0);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);

      await signal.reload();

      expect(calls, 2);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);
    });

    test('check refresh calls', () async {
      int calls = 0;

      Stream<int> stream() async* {
        calls++;
        await Future.delayed(const Duration(milliseconds: 5));
        yield* _stream();
      }

      final signal = streamSignal(() => stream());
      expect(signal.peek().isLoading, true);
      expect(calls, 0);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);

      await signal.refresh();

      expect(calls, 2);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);
    });

    test('onDone', () async {
      final stream = _stream();
      bool done = false;
      final signal = streamSignal(
        () => stream,
        onDone: () => done = true,
      );
      await signal.future;
      await signal.cancel();

      expect(done, true);
      expect(signal.isDone, true);
    });

    test('first', () async {
      final stream = _stream();
      final signal = streamSignal(() => stream);
      final value = await signal.first;

      expect(value, 10);
    });

    test('last', () async {
      final stream = _stream();
      final signal = streamSignal(() => stream);
      final value = await signal.last;

      expect(value, 10);
    });

    test('initial data', () {
      final signal = streamSignal(() => _stream(), initialValue: 0);
      expect(signal.peek().requireValue, 0);
    });

    test('not lazy', () {
      final signal = streamSignal(() => _stream(), lazy: false);
      expect(signal.peek().isLoading, true);
    });

    test('error', () async {
      bool error = false;
      final signal = streamSignal<int>(() async* {
        throw Exception();
      });
      effect(() {
        if (signal.value.hasError) {
          error = true;
        }
      });
      await Future.delayed(const Duration(milliseconds: 5));

      expect(error, true);
    });

    test('pause', () async {
      final signal = streamSignal(() => _stream());
      await signal.future;
      signal.pause();
      expect(signal.isPaused, true);
    });

    test('resume', () {
      final signal = streamSignal(() => _stream());
      signal.pause();
      signal.resume();
      expect(signal.isPaused, false);
    });

    test('cancel', () async {
      final signal = streamSignal(() => _stream());
      await signal.cancel();
      expect(signal.isDone, true);
    });

    test('onDone', () async {
      bool done = false;
      final signal = streamSignal(
        () => _stream(),
        onDone: () => done = true,
      );
      await signal.future;
      await signal.cancel();
      expect(done, true);
    });

    test('dispose', () async {
      final signal = streamSignal(() => _stream());
      signal.dispose();
      expect(signal.disposed, true);
    });

    test('cancelOnError', () async {
      final signal = streamSignal(
        () => _stream(true),
        cancelOnError: true,
      );

      signal.value;

      await Future.delayed(const Duration(milliseconds: 10));

      expect(signal.peek().error, isA<Exception>());
      expect(signal.isDone, true);
    });

    test('deprecated parameters and fallback options', () async {
      final s = streamSignal(
        () => _stream(),
        initialValue: 0,
        lazy: false,
        autoDispose: true,
        debugLabel: 'stream-dep',
      );
      expect(s.peek().value, 0);
      await s.future;
      expect(s.peek().value, 10);
    });

    test('cancel detaches implicit and explicit dependencies synchronously',
        () async {
      var implicitWatchers = 0;
      var explicitWatchers = 0;
      final identity = signal(
        'a',
        options: SignalOptions(
          watched: () => implicitWatchers++,
          unwatched: () => implicitWatchers--,
        ),
      );
      final revision = signal(
        0,
        options: SignalOptions(
          watched: () => explicitWatchers++,
          unwatched: () => explicitWatchers--,
        ),
      );
      final cleanup = Completer<void>();
      final controller = StreamController<int>(
        sync: true,
        onCancel: () => cleanup.future,
      );
      var calls = 0;
      final s = streamSignal(
        () {
          identity.value;
          calls++;
          return controller.stream;
        },
        options: AsyncSignalOptions(dependencies: [revision]),
      );
      addTearDown(s.dispose);
      s.value;
      controller.add(3);
      expect((implicitWatchers, explicitWatchers), (1, 1));

      final cancellation = s.cancel();
      expect((implicitWatchers, explicitWatchers), (0, 0));
      expect(controller.hasListener, false);
      identity.value = 'b';
      revision.value = 1;
      expect(s.value.value, 3);
      expect(s.peek().value, 3);
      expect(calls, 1);
      expect(s.disposed, false);
      s.setValue(9);
      expect(s.value.value, 9);
      expect(s.isDone, true);

      cleanup.complete();
      await cancellation;
      await controller.close();
    });

    test('cancel before first read leaves retained state inert and writable',
        () async {
      final identity = signal('a');
      var calls = 0;
      final s = streamSignal(
        () {
          identity.value;
          calls++;
          return Stream.value(7);
        },
        options: const AsyncSignalOptions(initialValue: 3),
      );
      addTearDown(s.dispose);
      await s.cancel();
      identity.value = 'b';
      expect(s.value.value, 3);
      expect(s.peek().value, 3);
      expect(s.value.value, 3);
      s.value = AsyncState.data(5);
      expect(s.peek().value, 5);
      expect(s.disposed, false);
      expect(calls, 0);
    });

    for (final restart in ['reset', 'reload', 'refresh']) {
      test('$restart restores cancelled dependency tracking without duplicates',
          () async {
        final identity = signal('a');
        final revision = signal(0);
        final controllers = <StreamController<String>>[];
        final sources = <String>[];
        final s = streamSignal(
          () {
            sources.add('${identity.value}:${revision.peek()}');
            final controller = StreamController<String>(sync: true);
            controllers.add(controller);
            return controller.stream;
          },
          options: AsyncSignalOptions(dependencies: [revision]),
        );
        final stop = effect(() => s.value);
        addTearDown(() async {
          stop();
          s.dispose();
          for (final controller in controllers) {
            await controller.close();
          }
        });
        controllers.single.add('old');
        await s.cancel();
        identity.value = 'b';
        revision.value = 1;
        switch (restart) {
          case 'reset':
            s.reset();
          case 'reload':
            await s.reload();
          case 'refresh':
            await s.refresh();
        }
        expect(sources, ['a:0', 'b:1']);
        expect(controllers.first.hasListener, false);
        expect(controllers.last.hasListener, true);
        controllers.last.add('b');
        expect(s.value.value, 'b');

        identity.value = 'c';
        expect(sources, ['a:0', 'b:1', 'c:1']);
        expect(s.value.isLoading, true);
        revision.value = 2;
        expect(sources, ['a:0', 'b:1', 'c:1', 'c:2']);
        controllers.last.add('current');
        expect(s.value.value, 'current');
        expect(controllers.where((c) => c.hasListener), [controllers.last]);
      });
    }

    test('explicit restarts reconnect the same broadcast source once',
        () async {
      var listens = 0;
      var cancellations = 0;
      final controller = StreamController<int>.broadcast(
        sync: true,
        onListen: () => listens++,
        onCancel: () => cancellations++,
      );
      var calls = 0;
      final s = streamSignal(() {
        calls++;
        return controller.stream;
      });
      final values = <int>[];
      final stop = effect(() {
        final state = s.value;
        if (state.hasValue) values.add(state.requireValue);
      });
      addTearDown(() async {
        stop();
        s.dispose();
        await controller.close();
      });
      controller.add(1);
      await s.cancel();
      s.reset();
      controller.add(2);
      await s.reload();
      controller.add(3);
      await s.refresh();
      controller.add(4);
      expect((calls, listens, cancellations), (4, 4, 3));
      expect(values.where((value) => value == 4), [4]);
      expect(s.value.value, 4);
    });

    test('failed cleanup still leaves cancellation detached', () async {
      final identity = signal('a');
      final failure = StateError('cleanup failed');
      final controller = StreamController<int>(
        onCancel: () => Future<void>.error(failure),
      );
      var calls = 0;
      final s = streamSignal(() {
        identity.value;
        calls++;
        return controller.stream;
      });
      addTearDown(s.dispose);
      s.value;
      await expectLater(s.cancel(), throwsA(same(failure)));
      identity.value = 'b';
      expect(s.value.isLoading, true);
      expect(s.peek().isLoading, true);
      expect(calls, 1);
      expect(s.isDone, true);
      expect(controller.hasListener, false);
      await controller.close();
    });

    test('identity-first observers never read the previous source value', () {
      final identity = signal('a');
      final a = StreamController<int>(sync: true);
      final b = StreamController<int>(sync: true);
      var calls = 0;
      final s = streamSignal(() {
        calls++;
        return identity.value == 'a' ? a.stream : b.stream;
      });
      final observations = <(String, int?)>[];
      final stop = effect(() {
        observations.add((identity.value, s.value.value));
      });
      addTearDown(() async {
        stop();
        s.dispose();
        await a.close();
        await b.close();
      });
      a.add(3);
      identity.value = 'b';
      expect(observations, [('a', null), ('a', 3), ('b', null)]);
      expect(a.hasListener, false);
      expect(b.hasListener, true);
      b.add(7);
      expect(observations.last, ('b', 7));
      expect(calls, 2);
    });

    test('old cancellation completion cannot release a restarted subscription',
        () async {
      final cleanup = Completer<void>();
      var newCancellations = 0;
      final old = StreamController<int>(onCancel: () => cleanup.future);
      final current = StreamController<int>(
        sync: true,
        onCancel: () => newCancellations++,
      );
      var calls = 0;
      final s = streamSignal(() => calls++ == 0 ? old.stream : current.stream);
      addTearDown(s.dispose);
      s.value;
      final cancellation = s.cancel();
      s.reset();
      expect(current.hasListener, true);
      cleanup.complete();
      await cancellation;
      expect(s.isDone, false);
      current.add(7);
      expect(s.value.value, 7);
      await s.cancel();
      expect(newCancellations, 1);
      expect(current.hasListener, false);
      await old.close();
      await current.close();
    });

    test('an error observer can switch sources without cancelling the new one',
        () {
      final retry = signal(false);
      final old = StreamController<int>(sync: true);
      final current = StreamController<int>(sync: true);
      final s = streamSignal(
        () => retry.value ? current.stream : old.stream,
        options: const AsyncSignalOptions(cancelOnError: true),
      );
      final stop = effect(() {
        if (s.value.hasError) retry.value = true;
      });
      addTearDown(() async {
        stop();
        s.dispose();
        await old.close();
        await current.close();
      });
      old.addError(StateError('retry'));
      expect(current.hasListener, true);
      expect(s.isDone, false);
      current.add(7);
      expect(s.value.value, 7);
    });

    test('completion and cancelOnError keep automatic source switching alive',
        () async {
      final identity = signal(0);
      final failure = StateError('source failed');
      final s = streamSignal(
        () {
          return switch (identity.value) {
            0 => const Stream<int>.empty(),
            1 => Stream<int>.error(failure),
            _ => Stream.value(7),
          };
        },
        options: const AsyncSignalOptions(cancelOnError: true),
      );
      addTearDown(s.dispose);
      s.value;
      await Future<void>.delayed(Duration.zero);
      expect(s.isDone, true);
      identity.value = 1;
      await Future<void>.delayed(Duration.zero);
      expect(s.value.error, same(failure));
      expect(s.isDone, true);
      identity.value = 2;
      expect(await s.future, 7);
    });

    test('dependencies containing AsyncSignal', () async {
      final controller = StreamController<String>.broadcast();
      final dep = asyncSignal<int>(AsyncState.data(1));
      final s = streamSignal(
        () => controller.stream,
        options: AsyncSignalOptions(dependencies: [dep]),
      );

      s.value; // trigger subscription
      controller.add('initial');
      await Future.delayed(Duration.zero);
      expect(s.value.value, 'initial');

      // Trigger dependency update to AsyncState.data(2)
      dep.value = AsyncState.data(2);

      controller.add('updated');
      await Future.delayed(Duration.zero);
      expect(s.value.value, 'updated');
    });

    test('dependencies containing AsyncSignal loading bypass', () async {
      final controller = StreamController<String>.broadcast();
      final dep = asyncSignal<int>(AsyncState.data(1));
      final s = streamSignal(
        () => controller.stream,
        options: AsyncSignalOptions(dependencies: [dep]),
      );

      s.value; // trigger subscription
      controller.add('initial');
      await Future.delayed(Duration.zero);
      expect(s.value.value, 'initial');

      // Transition to loading
      dep.value = AsyncState.loading();
      // Transition from loading to data(2) (triggers the isLoading && !isLoading early return)
      dep.value = AsyncState.data(2);

      controller.add('bypass');
      await Future.delayed(Duration.zero);
      expect(s.value.value, 'bypass');
    });
  });
}

Stream<int> _stream([bool error = false]) async* {
  await Future.delayed(const Duration(milliseconds: 5));
  if (!error) {
    yield 10;
  } else {
    throw Exception();
  }
}

Stream<String> _string(String prefix, int count) async* {
  for (var i = 0; i < count; i++) {
    yield '$prefix$i';
  }
}
