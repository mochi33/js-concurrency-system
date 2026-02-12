import { Registry } from "../src/registry.ts";

const registry = new Registry();

// Lazy registration — modules are imported only when the task is first executed
registry.lazy("heavyCalc", () => import("./tasks/heavy_calc.ts"));
registry.lazy("multiply", () => import("./tasks/multiply.ts"));
registry.lazy("echo", () => import("./tasks/echo.ts"));

export default registry;
