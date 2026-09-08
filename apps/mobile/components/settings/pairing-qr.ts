import qrcode from "qrcode-generator";

export function pairingQrGeometry(value: string) {
  const qr = qrcode(0, "M");
  qr.addData(value);
  qr.make();
  const size = qr.getModuleCount();
  let path = "";
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      if (qr.isDark(y, x)) path += `M${x + 4},${y + 4}h1v1h-1z`;
    }
  }
  return { size: size + 8, path };
}
