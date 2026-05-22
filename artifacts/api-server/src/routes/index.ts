import { Router, type IRouter } from "express";
import healthRouter from "./health.js";
import candlesRouter from "./candles.js";
import instrumentsRouter from "./instruments.js";

const router: IRouter = Router();

router.use(healthRouter);
router.use(candlesRouter);
router.use(instrumentsRouter);

export default router;
