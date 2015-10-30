/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.gradle.hadoopdsl;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the NamedScope class and the BaseNamedScopeContainer lookup method.
 */
class NamedScopeTest {
  @Test
  public void testFullyQualifiedLookup() {
    NamedScopeContainerWrapper global = new NamedScopeContainerWrapper(null, "");
    global.scope.bind("foo", 1);
    global.scope.bind("bar", 2);
    global.scope.bind("bazz", 3);

    NamedScopeContainerWrapper child = new NamedScopeContainerWrapper(global.scope, "child");
    global.scope.bind("child", child);
    child.scope.bind("bar", 22);
    child.scope.bind("foobar", 4);

    NamedScopeContainerWrapper grandChild = new NamedScopeContainerWrapper(child.scope, "grandChild");
    child.scope.bind("grandChild", grandChild);
    grandChild.scope.bind("foobar", 44);
    grandChild.scope.bind("foobarbazz", 5);

    assert(global.lookup(".foo") == 1);
    assert(global.lookup(".bar") == 2);
    assert(global.lookup(".bazz") == 3);
    assert(global.lookup(".child.bar") == 22);
    assert(global.lookup(".child.foobar") == 4);
    assert(global.lookup(".child.grandChild.foobar") == 44);
    assert(global.lookup(".child.grandChild.foobarbazz") == 5);

    assert(child.lookup(".foo") == 1);
    assert(child.lookup(".bar") == 2);
    assert(child.lookup(".bazz") == 3);
    assert(child.lookup(".child.bar") == 22);
    assert(child.lookup(".child.foobar") == 4);
    assert(child.lookup(".child.grandChild.foobar") == 44);
    assert(child.lookup(".child.grandChild.foobarbazz") == 5);

    assert(grandChild.lookup(".foo") == 1);
    assert(grandChild.lookup(".bar") == 2);
    assert(grandChild.lookup(".bazz") == 3);
    assert(grandChild.lookup(".child.bar") == 22);
    assert(grandChild.lookup(".child.foobar") == 4);
    assert(grandChild.lookup(".child.grandChild.foobar") == 44);
    assert(grandChild.lookup(".child.grandChild.foobarbazz") == 5);
  }

  @Test
  public void testQualifiedLookup() {
    NamedScopeContainerWrapper global = new NamedScopeContainerWrapper(null, "");
    global.scope.bind("foo", 1);
    global.scope.bind("bar", 2);
    global.scope.bind("bazz", 3);

    NamedScopeContainerWrapper child = new NamedScopeContainerWrapper(global.scope, "child");
    global.scope.bind("child", child);
    child.scope.bind("bar", 22);
    child.scope.bind("foobar", 4);

    NamedScopeContainerWrapper grandChild = new NamedScopeContainerWrapper(child.scope, "grandChild");
    child.scope.bind("grandChild", grandChild);
    grandChild.scope.bind("foobar", 44);
    grandChild.scope.bind("foobarbazz", 5);

    assert(global.lookup("child.foobar") == 4);
    assert(global.lookup("child.grandChild.foobar") == 44);
    assert(global.lookup("child.grandChild.foobarbazz") == 5);

    assert(child.lookup("child.foobar") == 4);
    assert(child.lookup("child.grandChild.foobar") == 44);
    assert(child.lookup("child.grandChild.foobarbazz") == 5);
    assert(child.lookup("grandChild.foobar") == 44);
    assert(child.lookup("grandChild.foobarbazz") == 5);

    assert(grandChild.lookup("child.foobar") == 4);
    assert(grandChild.lookup("child.grandChild.foobar") == 44);
    assert(grandChild.lookup("child.grandChild.foobarbazz") == 5);
    assert(grandChild.lookup("grandChild.foobar") == 44);
    assert(grandChild.lookup("grandChild.foobarbazz") == 5);
  }

  @Test
  public void testQualifiedNameAndScope() {
    NamedScope global = new NamedScope("", null);
    assert(global.getQualifiedName().equals(""));
    assert(global.findGlobalScope() == global);

    NamedScope child = new NamedScope("foo", global);
    assert(child.getQualifiedName().equals("foo"));
    assert(child.findGlobalScope() == global);

    NamedScope grandChild = new NamedScope("bar", child);
    assert(grandChild.getQualifiedName().equals("foo.bar"));
    assert(grandChild.findGlobalScope() == global);
  }

  @Test
  public void testUnqualifiedLookup() {
    NamedScopeContainerWrapper global = new NamedScopeContainerWrapper(null, "");
    global.scope.bind("foo", 1);
    global.scope.bind("bar", 2);
    global.scope.bind("bazz", 3);

    NamedScopeContainerWrapper child = new NamedScopeContainerWrapper(global.scope, "child");
    global.scope.bind("child", child);
    child.scope.bind("bar", 22);
    child.scope.bind("foobar", 4);

    NamedScopeContainerWrapper grandChild = new NamedScopeContainerWrapper(child.scope, "grandChild");
    child.scope.bind("grandChild", grandChild);
    grandChild.scope.bind("foobar", 44);
    grandChild.scope.bind("foobarbazz", 5);

    assert(child.lookup("foo") == 1);
    assert(child.lookup("bar") == 22);
    assert(child.lookup("bazz") == 3);
    assert(child.lookup("foobar") == 4);
    assert(child.scope.lookup("foobarbazz") == null);

    assert(grandChild.lookup("foo") == 1);
    assert(grandChild.lookup("bar") == 22);
    assert(grandChild.lookup("bazz") == 3);
    assert(grandChild.lookup("foobar") == 44);
    assert(grandChild.lookup("foobarbazz") == 5);
  }
}