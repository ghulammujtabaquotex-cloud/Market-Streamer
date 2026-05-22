import { Router, type IRouter, type Request, type Response } from "express";
import { tradowixWs } from "../lib/tradowix-ws.js";

const router: IRouter = Router();

router.get("/instruments", (_req: Request, res: Response) => {
  const all = tradowixWs.getInstruments();
  const open = all.filter((i) => i.isOpen).length;

  res.json({
    instruments: all,
    total: all.length,
    open,
  });
});

export default router;
