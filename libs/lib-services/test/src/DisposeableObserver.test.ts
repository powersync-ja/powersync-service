import { describe, expect, test } from 'vitest';

import { DisposableListener, DisposableObserver } from '../../src/utils/DisposableObserver.js';

describe('DisposableObserver', () => {
  test('it should dispose all listeners on dispose', () => {
    const listener = new DisposableObserver();

    let wasDisposed = false;
    listener.registerListener({
      disposed: () => {
        wasDisposed = true;
      }
    });

    listener[Symbol.dispose]();

    expect(wasDisposed).equals(true);
    expect(Object.keys(listener['listeners']).length).equals(0);
  });

  test('it should dispose nested listeners for managed listeners', () => {
    interface ParentListener extends DisposableListener {
      childCreated: (child: DisposableObserver<any>) => void;
    }
    class ParentObserver extends DisposableObserver<ParentListener> {
      createChild() {
        const child = new DisposableObserver();
        this.iterateListeners((cb) => cb.childCreated?.(child));
      }
    }

    const parent = new ParentObserver();
    let aChild: DisposableObserver<any> | null = null;

    parent.registerListener({
      childCreated: (child) => {
        aChild = child;
        child.registerManagedListener(parent, {
          test: () => {
            // this does nothing
          }
        });
      }
    });

    parent.createChild();

    // The managed listener should add a `disposed` listener
    expect(Object.keys(parent['listeners']).length).equals(2);
    expect(Object.keys(aChild!['listeners']).length).equals(1);

    parent[Symbol.dispose]();
    expect(Object.keys(parent['listeners']).length).equals(0);
    // The listener attached to the child should be disposed when the parent was disposed
    expect(Object.keys(aChild!['listeners']).length).equals(0);
  });
});
