/**
 * Brenn frontend entry point.
 * Imports the app component which self-registers and handles everything.
 *
 * global-error-handlers.js is imported first so its window listeners are
 * registered before app.js evaluates and BrennApp is constructed. ES module
 * imports are statically hoisted, so this ordering guarantee is spec-correct.
 */

import "./global-error-handlers.js";
import "./components/app.js";
