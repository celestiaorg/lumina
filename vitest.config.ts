// https://vitest.dev/guide/browser/

import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";

const exclude = ["**/{.git,.direnv,node_modules,target,dist}/**"];

export default defineConfig({
  test: {
    projects: [
      // Run tests in browser
      {
        plugins: [
          wasm(),
        ],
        test: {
          exclude: exclude,
          browser: {
            enabled: true,
            headless: true,
            screenshotFailures: false,
            ui: false,
            provider: "playwright",
            instances: [
              { browser: "chromium" },
              { browser: "firefox" },
              { browser: "webkit" },
            ],
          },
        },
      },
      // Run typechecks on testfiles
      {
        plugins: [
          wasm(),
        ],
        test: {
          typecheck: {
            exclude: exclude,
            enabled: true,
            only: true,
            // only check types for the test files, but not in deps
            ignoreSourceErrors: true,
          },
        }
      }
    ]
  }
});
