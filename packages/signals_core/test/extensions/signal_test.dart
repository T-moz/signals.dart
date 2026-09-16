import 'package:signals_core/signals_core.dart';
import 'package:test/test.dart';

void main() {
  SignalsObserver.instance = null;
  group('signal extensions', () {
    test('select', () {
      final a = signal({'a': 1});
      final b = a.select((e) => e.value.keys);

      expect(a.value.keys, b.value);
    });

    test('hooks', () {
      final counter = signal(0);
      final (getCount, setCount) = counter.hooks;

      expect(getCount(), 0);

      setCount(1);

      expect(getCount(), 1);
    });

    group('toStream', () {
      test('upstream data updates survive cancelled, disposed computed stream',
          () async {
        final source = asyncSignal(AsyncState<int>.data(0));
        final derived = computed(() => source.value);
        addTearDown(source.dispose);
        addTearDown(derived.dispose);

        expect((await derived.toStream().first).requireValue, 0);
        derived.dispose();

        source.setValue(1);
        expect(source.requireValue, 1);
        source.setValue(2);
        expect(source.requireValue, 2);
      });

      test('upstream errors survive cancelled, disposed computed stream',
          () async {
        final source = asyncSignal(AsyncState<int>.data(0));
        final derived = computed(() => source.value);
        addTearDown(source.dispose);
        addTearDown(derived.dispose);

        expect((await derived.toStream().first).requireValue, 0);
        derived.dispose();

        source.setError('error');
        expect(source.value.error, 'error');
        source.setValue(1);
        expect(source.requireValue, 1);
      });

      for (final autoDispose in [false, true]) {
        test('disposes once and completes with autoDispose=$autoDispose',
            () async {
          final source = signal(0);
          final derived = computed(
            () => source.value,
            options: ComputedOptions(autoDispose: autoDispose),
          );
          addTearDown(source.dispose);
          addTearDown(derived.dispose);
          var cleanups = 0;
          derived.onDispose(() => cleanups++);
          final events = expectLater(
            derived.toStream(),
            emitsInOrder([0, 1, emitsDone]),
          );

          source.value = 1;
          derived.dispose();

          await events;
          expect(cleanups, 1);
        });
      }
    });
  });
}
