import {
  resolveAdaptiveMaterialBlurIntensity,
  resolveAdaptiveMaterialMode,
  resolveAdaptiveMaterialOverlayColor,
  resolveLiquidGlassTintColor,
} from "../components/ui/adaptiveMaterialPolicy";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};

function assertEquals<T>(actual: T, expected: T) {
  if (actual !== expected) {
    throw new Error(`Expected ${String(expected)}, received ${String(actual)}`);
  }
}

Deno.test("uses Liquid Glass only when the complete iOS API is available", () => {
  assertEquals(
    resolveAdaptiveMaterialMode({
      platform: "ios",
      reduceTransparency: false,
      glassApiAvailable: true,
      liquidGlassAvailable: true,
    }),
    "liquid-glass",
  );
});

Deno.test("uses blur for iOS API fallback and web", () => {
  assertEquals(
    resolveAdaptiveMaterialMode({
      platform: "ios",
      reduceTransparency: false,
      glassApiAvailable: false,
      liquidGlassAvailable: false,
    }),
    "blur",
  );
  assertEquals(
    resolveAdaptiveMaterialMode({
      platform: "web",
      reduceTransparency: false,
      glassApiAvailable: false,
      liquidGlassAvailable: false,
    }),
    "blur",
  );
});

Deno.test("uses a solid surface when transparency is reduced", () => {
  assertEquals(
    resolveAdaptiveMaterialMode({
      platform: "ios",
      reduceTransparency: true,
      glassApiAvailable: true,
      liquidGlassAvailable: true,
    }),
    "solid",
  );
  assertEquals(
    resolveAdaptiveMaterialMode({
      platform: "android",
      reduceTransparency: true,
      glassApiAvailable: false,
      liquidGlassAvailable: false,
    }),
    "solid",
  );
});

Deno.test("uses Material blur on Android when transparency is allowed", () => {
  assertEquals(
    resolveAdaptiveMaterialMode({
      platform: "android",
      reduceTransparency: false,
      glassApiAvailable: false,
      liquidGlassAvailable: false,
    }),
    "blur",
  );
});

Deno.test("material tones form a restrained blur progression", () => {
  assertEquals(resolveAdaptiveMaterialBlurIntensity("subtle", 20, 40), 14);
  assertEquals(resolveAdaptiveMaterialBlurIntensity("regular", 20, 40), 20);
  assertEquals(resolveAdaptiveMaterialBlurIntensity("elevated", 20, 40), 30);
  assertEquals(resolveAdaptiveMaterialBlurIntensity("strong", 20, 40), 40);
});

Deno.test("liquid glass never receives a stacked scrim", () => {
  const surfaces = {
    glassBackground: "glass",
    glassBackgroundElevated: "glass-elevated",
    glassBackgroundPressed: "glass-pressed",
  };

  assertEquals(
    resolveAdaptiveMaterialOverlayColor("liquid-glass", "subtle", surfaces),
    undefined,
  );
  assertEquals(
    resolveAdaptiveMaterialOverlayColor("liquid-glass", "regular", surfaces),
    undefined,
  );
  assertEquals(
    resolveAdaptiveMaterialOverlayColor("liquid-glass", "strong", surfaces),
    undefined,
  );
});

Deno.test("blur fallback uses translucent glass fills, not opaque scrims", () => {
  const surfaces = {
    glassBackground: "glass",
    glassBackgroundElevated: "glass-elevated",
    glassBackgroundPressed: "glass-pressed",
  };

  assertEquals(
    resolveAdaptiveMaterialOverlayColor("blur", "subtle", surfaces),
    "glass",
  );
  assertEquals(
    resolveAdaptiveMaterialOverlayColor("blur", "regular", surfaces),
    "glass",
  );
  assertEquals(
    resolveAdaptiveMaterialOverlayColor("blur", "elevated", surfaces),
    "glass-elevated",
  );
  assertEquals(
    resolveAdaptiveMaterialOverlayColor("blur", "strong", surfaces),
    "glass-pressed",
  );
});

Deno.test("liquid glass stays lightly tinted per tone", () => {
  const surfaces = {
    glassBackground: "glass",
    glassBackgroundElevated: "glass-elevated",
    glassBackgroundPressed: "glass-pressed",
  };

  assertEquals(resolveLiquidGlassTintColor("subtle", surfaces), undefined);
  assertEquals(resolveLiquidGlassTintColor("regular", surfaces), "glass");
  assertEquals(
    resolveLiquidGlassTintColor("elevated", surfaces),
    "glass-elevated",
  );
  assertEquals(
    resolveLiquidGlassTintColor("strong", surfaces),
    "glass-pressed",
  );
});
